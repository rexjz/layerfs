//! `ExternalContent`: pack bodies appended, in publication order, to one file.
//!
//! I1: committed packs, ordered by `content_offset`, tile `[0, E)` exactly;
//! the `pack_content_append` trigger refuses any other offset.
//! I2: the file is at least E bytes long; the bytes past E belong to no row.
//! A publication writes its bodies from E (over any unreferenced tail), syncs
//! the file under the Durable profile and only then inserts the rows, so a
//! committed row never names content that was not written first.
use super::{connection::SqlWork, query, transaction::Transaction};
use crate::backend::{
    metadata_locations::info,
    records::{BackendError, Param, Record},
};
use layerfs_storage::{
    location::{PackDomain, PackInfo},
    port::{PersistenceError, PublishedPack},
};
use rusqlite::Connection;
use std::{cell::RefCell, fs::File, os::unix::fs::FileExt, time::Instant};

const END: &str = "SELECT content_offset+length FROM pack ORDER BY content_offset DESC LIMIT 1";

fn end_of(rows: &[Record]) -> Result<u64, BackendError> {
    match rows.first() {
        None => Ok(0),
        Some(r) => u64::try_from(r.get::<i64>(0)?).map_err(|_| BackendError::Integrity),
    }
}

/// E outside a transaction: the open-time I2 check.
pub(crate) fn committed_end(c: &Connection, w: &RefCell<SqlWork>) -> Result<u64, BackendError> {
    end_of(&query::run(c, END, vec![], w)?)
}

/// E inside a transaction.
pub(crate) fn end(tx: &Transaction<'_>) -> Result<u64, BackendError> {
    end_of(&tx.query(END, vec![])?)
}

/// Appends every body from E, then inserts the pack rows in bounded pages.
pub(crate) fn write_packs(
    tx: &Transaction<'_>,
    packs: &[PublishedPack],
) -> Result<(), BackendError> {
    if packs.is_empty() {
        return Ok(());
    }
    let file = tx.content()?;
    let mut offset = end(tx)?;
    let mut placed = Vec::with_capacity(packs.len());
    for pack in packs {
        if pack.body.len() != pack.info.length {
            return Err(BackendError::Integrity);
        }
        file.write_all_at(&pack.body, offset)
            .map_err(|_| BackendError::Unknown)?;
        placed.push((pack, offset));
        offset += pack.body.len() as u64;
    }
    if tx.durable() {
        file.sync_data().map_err(|_| BackendError::Unknown)?;
    }
    const PREFIX: &str = "INSERT INTO pack(pack_id,domain,digest,length,content_offset) VALUES ";
    const ROW: &str = "(?,?,?,?,?)";
    let limit = tx
        .input_limit(5, PREFIX.len(), ROW.len() + 1)?
        .min(layerfs_storage::policy::BATCH_OBJECT_LIMIT);
    for page in placed.chunks(limit.max(1)) {
        let sql = format!(
            "{PREFIX}{}",
            std::iter::repeat_n(ROW, page.len())
                .collect::<Vec<_>>()
                .join(",")
        );
        let params = page
            .iter()
            .flat_map(|(p, offset)| {
                [
                    Param::I64(p.info.pack_id),
                    Param::I64(if p.info.domain == PackDomain::Metadata {
                        0
                    } else {
                        1
                    }),
                    Param::Bytes(p.info.key.as_bytes().to_vec()),
                    Param::I64(p.info.length as i64),
                    Param::I64(*offset as i64),
                ]
            })
            .collect();
        tx.query(&sql, params)?;
        let mut w = tx.work.borrow_mut();
        w.sealed_inserts += page.len() as u64;
        w.sealed_body_bytes += page.iter().map(|(p, _)| p.body.len() as u64).sum::<u64>();
    }
    Ok(())
}

/// The descriptor and content offset of one committed pack.
pub(crate) fn locate(tx: &Transaction<'_>, id: i64) -> Result<(PackInfo, u64), PersistenceError> {
    let rows = tx.query(
        "SELECT pack_id,domain,digest,length,content_offset FROM pack WHERE pack_id=?1",
        vec![Param::I64(id)],
    )?;
    let row = rows.first().ok_or(PersistenceError::Missing)?;
    let descriptor = info(row, 0)?;
    let offset = u64::try_from(row.get::<i64>(4)?).map_err(|_| BackendError::Integrity)?;
    if rows.len() != 1 || descriptor.pack_id != id {
        return Err(PersistenceError::Malformed);
    }
    Ok((descriptor, offset))
}

/// Reads `bytes.len()` bytes at `at` inside the pack whose content starts at `base`.
pub(crate) fn read_at(
    tx: &Transaction<'_>,
    file: &File,
    base: u64,
    length: usize,
    at: usize,
    bytes: &mut [u8],
) -> Result<(), PersistenceError> {
    if at.checked_add(bytes.len()).is_none_or(|end| end > length) {
        return Err(PersistenceError::Malformed);
    }
    {
        let mut work = tx.work.borrow_mut();
        work.blob_read_calls += 1;
        work.blob_requested_bytes += bytes.len() as u64;
    }
    let start = Instant::now();
    let result = file.read_exact_at(bytes, base + at as u64);
    let mut work = tx.work.borrow_mut();
    work.blob_read_ns += start.elapsed().as_nanos() as u64;
    match result {
        Ok(()) => {
            work.blob_read_bytes += bytes.len() as u64;
            Ok(())
        }
        // A committed pack past the end of the file breaks I2.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(BackendError::Integrity.into())
        }
        Err(_) => Err(BackendError::Unknown.into()),
    }
}

/// One whole committed body, read from the content file.
pub(crate) fn body(tx: &Transaction<'_>, info: &PackInfo) -> Result<Vec<u8>, PersistenceError> {
    let (located, offset) = locate(tx, info.pack_id)?;
    if located != *info {
        return Err(PersistenceError::Malformed);
    }
    let mut body = vec![0; info.length];
    read_at(tx, tx.content()?, offset, info.length, 0, &mut body)?;
    Ok(body)
}

/// Truncates the unreferenced tail and syncs: only for the sole-owner seal.
pub(crate) fn trim(c: &Connection, w: &RefCell<SqlWork>, file: &File) -> Result<u64, BackendError> {
    let end = committed_end(c, w)?;
    let length = file.metadata().map_err(|_| BackendError::Unknown)?.len();
    if length < end {
        return Err(BackendError::Integrity);
    }
    if length > end {
        file.set_len(end).map_err(|_| BackendError::Unknown)?;
    }
    file.sync_all().map_err(|_| BackendError::Unknown)?;
    Ok(end)
}
