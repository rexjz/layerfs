//! Incremental change export between replicas of one Store.
//!
//! A Store marked as an export base logs the primary key of every row its
//! writers insert, update or delete in the logged tables. Schema triggers fill
//! `change_log`, so every opener's writes are logged without a code path of
//! its own, in the single writer's commit order. A Store that was never marked
//! logs nothing.
//!
//! [`export_changes`] reads one snapshot of a live Store and writes every row
//! changed after a cursor, at its current value, into a new bundle file: a
//! SQLite file with the logged tables' exact definitions, the deleted keys and
//! the bundle's two cursors. [`apply_changes`] installs a bundle into an
//! offline replica at the bundle's starting cursor, in one transaction.
//! [`mark_base`] makes an offline Store (normally a sealed file) the start of a
//! chain.
//!
//! A cursor names one replica state: a 32-byte state identity and the change
//! sequence it reaches. Each bundle's ending state derives from its starting
//! state and the digest of its contents, so two replicas that diverge from one
//! state never share a later state, and a bundle applies only to the state it
//! was exported from.
use rusqlite::{params, types::ValueRef, Connection, OpenFlags, OptionalExtension};
use std::{fmt, fs, io, os::unix::fs::FileExt, path::Path};

/// Bundle format written and read by this module.
pub const BUNDLE_FORMAT: i64 = 1;

/// One replica state: its identity and the change sequence it reaches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangeCursor {
    /// State identity.
    pub state: [u8; 32],
    /// Last change sequence this state includes.
    pub seq: u64,
}

/// What one export read and wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportReceipt {
    /// The cursor the bundle starts from.
    pub before: ChangeCursor,
    /// The cursor the bundle reaches; equal to `before` when nothing changed.
    pub after: ChangeCursor,
    /// Rows written at their current value.
    pub rows: u64,
    /// Keys written as deleted.
    pub deletes: u64,
    /// Change log entries read in the cursor range.
    pub log_entries: u64,
    /// Bundle file length; 0 when nothing changed and no file was written.
    pub bytes: u64,
    /// Digest of the bundle's rows and deletions.
    pub digest: [u8; 32],
}

/// What one apply installed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyReceipt {
    /// The replica's cursor before the bundle.
    pub before: ChangeCursor,
    /// The replica's cursor after the bundle.
    pub after: ChangeCursor,
    /// Rows installed.
    pub rows: u64,
    /// Keys deleted.
    pub deletes: u64,
}

/// Why an export, apply or base mark was refused or failed.
#[derive(Debug)]
pub enum ChangeError {
    /// The Store is not an export base: it records no change state.
    NotBase,
    /// The cursor does not name a state this Store or bundle can use.
    Cursor(&'static str),
    /// The replica was written after its recorded state.
    Diverged,
    /// The Store or bundle is not consistent.
    Integrity(&'static str),
    /// The bundle path already exists.
    Exists,
    /// SQLite failed.
    Sqlite(rusqlite::Error),
    /// The filesystem failed.
    Io(io::Error),
}

impl fmt::Display for ChangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBase => write!(f, "the Store is not an export base"),
            Self::Cursor(why) => write!(f, "cursor refused: {why}"),
            Self::Diverged => write!(f, "the replica was written after its recorded state"),
            Self::Integrity(why) => write!(f, "integrity: {why}"),
            Self::Exists => write!(f, "the bundle path already exists"),
            Self::Sqlite(error) => write!(f, "sqlite: {error}"),
            Self::Io(error) => write!(f, "io: {error}"),
        }
    }
}

impl std::error::Error for ChangeError {}

impl From<rusqlite::Error> for ChangeError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<io::Error> for ChangeError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// One logged table: its stable code, name and primary key.
struct Logged {
    code: i64,
    table: &'static str,
    keys: &'static [&'static str],
    /// Rows are never updated or deleted (published packs).
    immutable: bool,
}

/// Logged tables in code order. Codes are part of the bundle format and of
/// `change_log.sql`; code 0 is the sequence marker.
const LOGGED: [Logged; 19] = [
    Logged {
        code: 1,
        table: "pack",
        keys: &["pack_id"],
        immutable: true,
    },
    Logged {
        code: 2,
        table: "pack_unit",
        keys: &["unit_id"],
        immutable: true,
    },
    Logged {
        code: 3,
        table: "object_location",
        keys: &["object_id"],
        immutable: false,
    },
    Logged {
        code: 4,
        table: "inode_value",
        keys: &["ordinal"],
        immutable: false,
    },
    Logged {
        code: 5,
        table: "content_signature",
        keys: &["slot"],
        immutable: false,
    },
    Logged {
        code: 6,
        table: "store_policy",
        keys: &["id"],
        immutable: false,
    },
    Logged {
        code: 7,
        table: "store_totals",
        keys: &["id"],
        immutable: false,
    },
    Logged {
        code: 8,
        table: "history_meta",
        keys: &["id"],
        immutable: false,
    },
    Logged {
        code: 9,
        table: "layer_stack",
        keys: &["layer_stack_id"],
        immutable: false,
    },
    Logged {
        code: 10,
        table: "layer",
        keys: &["layer_id"],
        immutable: false,
    },
    Logged {
        code: 11,
        table: "commit",
        keys: &["commit_id"],
        immutable: false,
    },
    Logged {
        code: 12,
        table: "branch",
        keys: &["branch_id"],
        immutable: false,
    },
    Logged {
        code: 13,
        table: "scope_allocator",
        keys: &["scope_id"],
        immutable: false,
    },
    Logged {
        code: 14,
        table: "binding_hint",
        keys: &["child", "parent", "name"],
        immutable: false,
    },
    Logged {
        code: 15,
        table: "layer_lineage",
        keys: &["layer_id"],
        immutable: false,
    },
    Logged {
        code: 16,
        table: "commit_lineage",
        keys: &["commit_id"],
        immutable: false,
    },
    Logged {
        code: 17,
        table: "absorbed",
        keys: &["run", "depth"],
        immutable: false,
    },
    Logged {
        code: 18,
        table: "reach",
        keys: &["key", "x"],
        immutable: false,
    },
    Logged {
        code: 19,
        table: "file_digest",
        keys: &["digest", "length"],
        immutable: false,
    },
];

/// `store_totals`' code. Inserting Commits and Layers adjusts the replica's
/// totals through its own triggers, so the bundled totals are installed last.
const TOTALS: i64 = 7;

/// The bundle's own tables, next to copies of the logged tables.
const BUNDLE_SCHEMA: &str = "
CREATE TABLE bundle_meta (
 id INTEGER PRIMARY KEY CHECK(id=1),
 format INTEGER NOT NULL,
 catalog_id BLOB NOT NULL,
 before_state BLOB NOT NULL CHECK(length(before_state)=32),
 before_seq INTEGER NOT NULL CHECK(before_seq>=0),
 after_state BLOB NOT NULL CHECK(length(after_state)=32),
 after_seq INTEGER NOT NULL CHECK(after_seq>before_seq),
 rows INTEGER NOT NULL CHECK(rows>=0),
 deletes INTEGER NOT NULL CHECK(deletes>=0),
 digest BLOB NOT NULL CHECK(length(digest)=32)
) STRICT;
CREATE TABLE bundle_delete (
 tbl INTEGER NOT NULL CHECK(tbl BETWEEN 1 AND 19),
 k1 ANY NOT NULL,
 k2 ANY,
 k3 ANY
) STRICT;
";

/// Whether `schema`'s `pack` table keeps its bodies in the content file.
fn external(c: &Connection, schema: &str) -> Result<bool, ChangeError> {
    Ok(c.query_row(
        "SELECT count(*) FROM pragma_table_info('pack',?1) WHERE name='content_offset'",
        [schema],
        |r| r.get::<_, i64>(0),
    )? == 1)
}

/// The bodies of a bundle's packs, for an `ExternalContent` Store.
const BUNDLE_CONTENT: &str = "CREATE TABLE bundle_content (
 pack_id INTEGER PRIMARY KEY,
 body BLOB NOT NULL
) STRICT;";

fn quoted(name: &str) -> String {
    format!("\"{name}\"")
}

/// `k1,...,kn` for a table's key width.
fn key_columns(width: usize) -> String {
    (1..=width)
        .map(|i| format!("k{i}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn table_exists(c: &Connection, schema: &str, name: &str) -> Result<bool, ChangeError> {
    Ok(c.query_row(
        &format!("SELECT 1 FROM {schema}.sqlite_schema WHERE type='table' AND name=?1"),
        [name],
        |_| Ok(()),
    )
    .optional()?
    .is_some())
}

fn state_of(c: &Connection, schema: &str) -> Result<Option<ChangeCursor>, ChangeError> {
    let row: Option<(Vec<u8>, i64)> = c
        .query_row(
            &format!("SELECT state,seq FROM {schema}.change_state WHERE id=1"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    row.map(|(state, seq)| {
        Ok(ChangeCursor {
            state: state
                .try_into()
                .map_err(|_| ChangeError::Integrity("change state length"))?,
            seq: u64::try_from(seq).map_err(|_| ChangeError::Integrity("change state sequence"))?,
        })
    })
    .transpose()
}

fn catalog_of(c: &Connection, schema: &str) -> Result<Vec<u8>, ChangeError> {
    Ok(c.query_row(
        &format!("SELECT catalog_id FROM {schema}.history_meta WHERE id=1"),
        [],
        |r| r.get(0),
    )?)
}

fn last_seq(c: &Connection, schema: &str) -> Result<Option<u64>, ChangeError> {
    let seq: Option<i64> = c.query_row(
        &format!("SELECT max(seq) FROM {schema}.change_log"),
        [],
        |r| r.get(0),
    )?;
    seq.map(|s| u64::try_from(s).map_err(|_| ChangeError::Integrity("change log sequence")))
        .transpose()
}

fn base_state(catalog: &[u8], seq: u64, token: &[u8; 32]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"layerfs-change-base-v1\0");
    h.update(&(catalog.len() as u64).to_le_bytes());
    h.update(catalog);
    h.update(&seq.to_le_bytes());
    h.update(token);
    h.finalize().into()
}

fn next_state(
    catalog: &[u8],
    before: &ChangeCursor,
    after_seq: u64,
    digest: &[u8; 32],
) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"layerfs-change-v1\0");
    h.update(&(catalog.len() as u64).to_le_bytes());
    h.update(catalog);
    h.update(&before.state);
    h.update(&before.seq.to_le_bytes());
    h.update(&after_seq.to_le_bytes());
    h.update(digest);
    h.finalize().into()
}

/// Feeds one value with its type tag and length.
fn feed(h: &mut blake3::Hasher, value: ValueRef<'_>) {
    match value {
        ValueRef::Null => {
            h.update(&[0]);
        }
        ValueRef::Integer(i) => {
            h.update(&[1]);
            h.update(&i.to_le_bytes());
        }
        ValueRef::Real(r) => {
            h.update(&[2]);
            h.update(&r.to_bits().to_le_bytes());
        }
        ValueRef::Text(t) => {
            h.update(&[3]);
            h.update(&(t.len() as u64).to_le_bytes());
            h.update(t);
        }
        ValueRef::Blob(b) => {
            h.update(&[4]);
            h.update(&(b.len() as u64).to_le_bytes());
            h.update(b);
        }
    }
}

/// The digest of the bundle tables in `schema`: for each logged table in code
/// order, its rows ordered by primary key, then its deleted keys in order.
/// Export and apply both compute it from the bundle file itself.
fn bundle_digest(c: &Connection, schema: &str) -> Result<([u8; 32], u64, u64), ChangeError> {
    let mut h = blake3::Hasher::new();
    h.update(b"layerfs-change-bundle-v1\0");
    let (mut rows, mut deletes) = (0u64, 0u64);
    for t in &LOGGED {
        if !table_exists(c, schema, t.table)? {
            continue;
        }
        h.update(&t.code.to_le_bytes());
        let mut stmt = c.prepare(&format!(
            "SELECT * FROM {schema}.{} ORDER BY {}",
            quoted(t.table),
            t.keys.join(",")
        ))?;
        let width = stmt.column_count();
        let mut cursor = stmt.query([])?;
        while let Some(row) = cursor.next()? {
            h.update(b"r");
            h.update(&(width as u32).to_le_bytes());
            for i in 0..width {
                feed(&mut h, row.get_ref(i)?);
            }
            rows += 1;
        }
        let columns = key_columns(t.keys.len());
        let mut stmt = c.prepare(&format!(
            "SELECT {columns} FROM {schema}.bundle_delete WHERE tbl=?1 ORDER BY {columns}"
        ))?;
        let mut cursor = stmt.query([t.code])?;
        while let Some(row) = cursor.next()? {
            h.update(b"d");
            for i in 0..t.keys.len() {
                feed(&mut h, row.get_ref(i)?);
            }
            deletes += 1;
        }
    }
    if table_exists(c, schema, "bundle_content")? {
        h.update(b"c");
        let mut stmt = c.prepare(&format!(
            "SELECT pack_id,body FROM {schema}.bundle_content ORDER BY pack_id"
        ))?;
        let mut cursor = stmt.query([])?;
        while let Some(row) = cursor.next()? {
            feed(&mut h, row.get_ref(0)?);
            feed(&mut h, row.get_ref(1)?);
        }
    }
    Ok((h.finalize().into(), rows, deletes))
}

/// Makes the offline Store at `path` the start of an export chain and returns
/// its cursor. Clears the change log down to one sequence marker and enables
/// logging. Marking again starts a new chain at the current sequence. `token`
/// makes the base identity unique; supply fresh random bytes.
pub fn mark_base(path: &Path, token: [u8; 32]) -> Result<ChangeCursor, ChangeError> {
    let c = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    c.execute_batch("BEGIN IMMEDIATE")?;
    let marked = (|| {
        if !table_exists(&c, "main", "change_log")? {
            return Err(ChangeError::Integrity(
                "the Store has no change log; open it writable once to migrate it",
            ));
        }
        let catalog = catalog_of(&c, "main")?;
        let recorded = state_of(&c, "main")?.map_or(0, |s| s.seq);
        let seq = last_seq(&c, "main")?.unwrap_or(0).max(recorded);
        c.execute("DELETE FROM change_log", [])?;
        if seq > 0 {
            c.execute("INSERT INTO change_log(seq,tbl) VALUES(?1,0)", [seq as i64])?;
        }
        let state = base_state(&catalog, seq, &token);
        c.execute(
            "INSERT INTO change_state(id,state,seq) VALUES(1,?1,?2) \
             ON CONFLICT(id) DO UPDATE SET state=excluded.state,seq=excluded.seq",
            params![&state[..], seq as i64],
        )?;
        Ok(ChangeCursor { state, seq })
    })();
    finish(&c, marked)
}

/// The change state recorded in the Store at `path`, if it is an export base.
pub fn read_state(path: &Path) -> Result<Option<ChangeCursor>, ChangeError> {
    let c = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    if !table_exists(&c, "main", "change_state")? {
        return Ok(None);
    }
    state_of(&c, "main")
}

/// Writes every row of the live Store at `store` changed after `since` into a
/// new bundle file at `bundle`, from one read snapshot. Takes no write lock
/// and does not stop writers. When nothing changed, writes no file and
/// returns `after == before`.
pub fn export_changes(
    store: &Path,
    since: ChangeCursor,
    bundle: &Path,
) -> Result<ExportReceipt, ChangeError> {
    let source = Connection::open_with_flags(
        store,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    source.execute_batch("BEGIN")?;
    let exported = export_in(&source, store, since, bundle);
    // Ends the read snapshot; a read transaction has nothing to commit.
    let _ = source.execute_batch("COMMIT");
    exported
}

fn export_in(
    source: &Connection,
    store: &Path,
    since: ChangeCursor,
    bundle: &Path,
) -> Result<ExportReceipt, ChangeError> {
    let base = state_of(source, "main")?.ok_or(ChangeError::NotBase)?;
    if since.seq < base.seq || (since.seq == base.seq && since.state != base.state) {
        return Err(ChangeError::Cursor(
            "the cursor precedes or differs from the Store's recorded state",
        ));
    }
    let last = last_seq(source, "main")?.unwrap_or(0).max(base.seq);
    if since.seq > last {
        return Err(ChangeError::Cursor("the cursor is ahead of the Store"));
    }
    if since.seq == last {
        return Ok(ExportReceipt {
            before: since,
            after: since,
            rows: 0,
            deletes: 0,
            log_entries: 0,
            bytes: 0,
            digest: [0; 32],
        });
    }
    let catalog = catalog_of(source, "main")?;
    let log_entries: i64 = source.query_row(
        "SELECT count(*) FROM change_log WHERE tbl>0 AND seq>?1 AND seq<=?2",
        [since.seq as i64, last as i64],
        |r| r.get(0),
    )?;
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(bundle)
    {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(ChangeError::Exists)
        }
        Err(error) => return Err(error.into()),
    }
    let content = if external(source, "main")? {
        Some(fs::File::open(crate::config::content_path(store))?)
    } else {
        None
    };
    let written = write_bundle(source, since, last, &catalog, bundle, content.as_ref());
    match written {
        Ok((after, digest, rows, deletes)) => Ok(ExportReceipt {
            before: since,
            after,
            rows,
            deletes,
            log_entries: log_entries as u64,
            bytes: fs::metadata(bundle)?.len(),
            digest,
        }),
        Err(error) => {
            let _ = fs::remove_file(bundle);
            Err(error)
        }
    }
}

fn write_bundle(
    source: &Connection,
    since: ChangeCursor,
    last: u64,
    catalog: &[u8],
    bundle: &Path,
    content: Option<&fs::File>,
) -> Result<(ChangeCursor, [u8; 32], u64, u64), ChangeError> {
    let out = Connection::open(bundle)?;
    // The bundle holds copies of some rows; the replica checks references
    // when the bundle is applied. The bundled SQLite enables them by default.
    // Small pages keep a bundle of a few rows small: every table it carries
    // takes at least one page. Large payloads lose 4 bytes per overflow page.
    out.execute_batch(
        "PRAGMA page_size=1024; PRAGMA foreign_keys=OFF; PRAGMA journal_mode=OFF; \
         PRAGMA synchronous=OFF; BEGIN;",
    )?;
    out.execute_batch(BUNDLE_SCHEMA)?;
    let mut logged = source
        .prepare("SELECT EXISTS(SELECT 1 FROM change_log WHERE tbl=?1 AND seq>?2 AND seq<=?3)")?;
    for t in &LOGGED {
        if !table_exists(source, "main", t.table)? {
            continue;
        }
        // A table carries its changed rows and its deleted keys; a table
        // with neither is left out of the bundle.
        let changed: bool =
            logged.query_row(params![t.code, since.seq as i64, last as i64], |r| r.get(0))?;
        if !changed {
            continue;
        }
        let definition: String = source.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
            [t.table],
            |r| r.get(0),
        )?;
        out.execute_batch(&definition)?;
        let columns = key_columns(t.keys.len());
        let join = t
            .keys
            .iter()
            .enumerate()
            .map(|(i, key)| format!("r.{key}=c.k{}", i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut read = source.prepare(&format!(
            "SELECT {columns},(r.{} IS NOT NULL),r.* FROM \
             (SELECT DISTINCT {columns} FROM change_log WHERE tbl=?1 AND seq>?2 AND seq<=?3) c \
             LEFT JOIN {} r ON {join}",
            t.keys[0],
            quoted(t.table)
        ))?;
        let offset = t.keys.len() + 1;
        let width = read.column_count() - offset;
        let mut insert = out.prepare(&format!(
            "INSERT INTO {} VALUES({})",
            quoted(t.table),
            vec!["?"; width].join(",")
        ))?;
        let mut delete = out.prepare(&format!(
            "INSERT INTO bundle_delete(tbl,{columns}) VALUES(?1,{})",
            (2..=t.keys.len() + 1)
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(",")
        ))?;
        let mut rows = read.query([t.code, since.seq as i64, last as i64])?;
        while let Some(row) = rows.next()? {
            let present: bool = row.get(t.keys.len())?;
            if present {
                let values = (offset..offset + width)
                    .map(|i| row.get_ref(i).map(rusqlite::types::ToSqlOutput::Borrowed))
                    .collect::<Result<Vec<_>, _>>()?;
                insert.execute(rusqlite::params_from_iter(values))?;
            } else if t.immutable {
                return Err(ChangeError::Integrity("a logged immutable row is absent"));
            } else {
                let mut values = vec![rusqlite::types::Value::Integer(t.code)];
                for i in 0..t.keys.len() {
                    values.push(row.get(i)?);
                }
                delete.execute(rusqlite::params_from_iter(values))?;
            }
        }
    }
    // ExternalContent: each bundled pack carries its body, read from the
    // content file; a committed pack's bytes never change once written.
    if let Some(file) = content {
        if table_exists(&out, "main", "pack")? {
            out.execute_batch(BUNDLE_CONTENT)?;
            let mut packs = out.prepare("SELECT pack_id,content_offset,length FROM pack")?;
            let mut insert =
                out.prepare("INSERT INTO bundle_content(pack_id,body) VALUES(?1,?2)")?;
            let mut cursor = packs.query([])?;
            while let Some(row) = cursor.next()? {
                let (id, offset, length): (i64, i64, i64) = (row.get(0)?, row.get(1)?, row.get(2)?);
                let mut body = vec![0; length as usize];
                file.read_exact_at(&mut body, offset as u64)?;
                insert.execute(params![id, body])?;
            }
        }
    }
    let (digest, rows, deletes) = bundle_digest(&out, "main")?;
    let after = ChangeCursor {
        state: next_state(catalog, &since, last, &digest),
        seq: last,
    };
    out.execute(
        "INSERT INTO bundle_meta(id,format,catalog_id,before_state,before_seq,after_state,after_seq,rows,deletes,digest) \
         VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            BUNDLE_FORMAT,
            catalog,
            &since.state[..],
            since.seq as i64,
            &after.state[..],
            after.seq as i64,
            rows as i64,
            deletes as i64,
            &digest[..]
        ],
    )?;
    out.execute_batch("COMMIT")?;
    Ok((after, digest, rows, deletes))
}

/// Installs the bundle at `bundle` into the offline replica at `store`, which
/// must be at the bundle's starting cursor and unwritten since. Verifies the
/// bundle's digest and state, installs every row and deletion in one
/// transaction with foreign keys checked at commit, and records the bundle's
/// ending cursor.
pub fn apply_changes(store: &Path, bundle: &Path) -> Result<ApplyReceipt, ChangeError> {
    let c = Connection::open_with_flags(
        store,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    c.execute_batch("PRAGMA foreign_keys=ON")?;
    let name = bundle
        .to_str()
        .ok_or(ChangeError::Integrity("the bundle path is not UTF-8"))?;
    c.execute("ATTACH DATABASE ?1 AS b", [name])?;
    c.execute_batch("BEGIN IMMEDIATE; PRAGMA defer_foreign_keys=ON;")?;
    let applied = apply_in(&c, store);
    let finished = finish(&c, applied);
    let _ = c.execute_batch("DETACH DATABASE b");
    finished
}

/// The `bundle_meta` row of an attached bundle, as stored.
struct BundleMeta {
    format: i64,
    catalog: Vec<u8>,
    before_state: Vec<u8>,
    before_seq: i64,
    after_state: Vec<u8>,
    after_seq: i64,
    rows: i64,
    deletes: i64,
    digest: Vec<u8>,
}

fn apply_in(c: &Connection, store: &Path) -> Result<ApplyReceipt, ChangeError> {
    let BundleMeta {
        format,
        catalog,
        before_state,
        before_seq,
        after_state,
        after_seq,
        rows,
        deletes,
        digest,
    } = c.query_row(
        "SELECT format,catalog_id,before_state,before_seq,after_state,after_seq,rows,deletes,digest \
         FROM b.bundle_meta WHERE id=1",
        [],
        |r| {
            Ok(BundleMeta {
                format: r.get(0)?,
                catalog: r.get(1)?,
                before_state: r.get(2)?,
                before_seq: r.get(3)?,
                after_state: r.get(4)?,
                after_seq: r.get(5)?,
                rows: r.get(6)?,
                deletes: r.get(7)?,
                digest: r.get(8)?,
            })
        },
    )?;
    if format != BUNDLE_FORMAT {
        return Err(ChangeError::Integrity("unknown bundle format"));
    }
    if catalog != catalog_of(c, "main")? {
        return Err(ChangeError::Integrity(
            "the bundle belongs to another catalog",
        ));
    }
    let to_state = |bytes: Vec<u8>| -> Result<[u8; 32], ChangeError> {
        bytes
            .try_into()
            .map_err(|_| ChangeError::Integrity("bundle state length"))
    };
    let before = ChangeCursor {
        state: to_state(before_state)?,
        seq: before_seq as u64,
    };
    let after = ChangeCursor {
        state: to_state(after_state)?,
        seq: after_seq as u64,
    };
    let current = state_of(c, "main")?.ok_or(ChangeError::NotBase)?;
    if current != before {
        return Err(ChangeError::Cursor(
            "the replica is not at the bundle's starting state",
        ));
    }
    if last_seq(c, "main")?.is_some_and(|seq| seq != before.seq) {
        return Err(ChangeError::Diverged);
    }
    let (computed, counted_rows, counted_deletes) = bundle_digest(c, "b")?;
    if computed[..] != digest[..]
        || counted_rows != rows as u64
        || counted_deletes != deletes as u64
    {
        return Err(ChangeError::Integrity("bundle digest"));
    }
    if next_state(&catalog, &before, after.seq, &computed) != after.state {
        return Err(ChangeError::Integrity("bundle ending state"));
    }
    let external = external(c, "main")?;
    if external {
        append_content(c, store)?;
    } else if table_exists(c, "b", "bundle_content")? {
        return Err(ChangeError::Integrity(
            "the bundle carries pack content for a Store that keeps none",
        ));
    }
    let order = LOGGED
        .iter()
        .filter(|t| t.code != TOTALS)
        .chain(LOGGED.iter().filter(|t| t.code == TOTALS));
    for t in order {
        if !table_exists(c, "b", t.table)? {
            continue;
        }
        if !table_exists(c, "main", t.table)? {
            return Err(ChangeError::Integrity(
                "a bundled table is absent from the replica",
            ));
        }
        let table = quoted(t.table);
        let key = t.keys.join(",");
        let columns = key_columns(t.keys.len());
        let deleted: i64 = c.query_row(
            "SELECT count(*) FROM b.bundle_delete WHERE tbl=?1",
            [t.code],
            |r| r.get(0),
        )?;
        if t.immutable {
            if deleted != 0 {
                return Err(ChangeError::Integrity("a bundle deletes an immutable row"));
            }
        } else {
            c.execute(
                &format!("DELETE FROM main.{table} WHERE ({key}) IN (SELECT {key} FROM b.{table})"),
                [],
            )?;
            c.execute(
                &format!(
                    "DELETE FROM main.{table} WHERE ({key}) IN \
                     (SELECT {columns} FROM b.bundle_delete WHERE tbl=?1)"
                ),
                [t.code],
            )?;
        }
        // Content offsets must arrive in file order for `pack_content_append`.
        let order = if external && t.table == "pack" {
            " ORDER BY content_offset"
        } else {
            ""
        };
        c.execute(
            &format!("INSERT INTO main.{table} SELECT * FROM b.{table}{order}"),
            [],
        )?;
    }
    // The installed rows logged themselves; the replica now stands at `after`.
    c.execute("DELETE FROM main.change_log", [])?;
    c.execute(
        "INSERT INTO main.change_log(seq,tbl) VALUES(?1,0)",
        [after.seq as i64],
    )?;
    c.execute(
        "UPDATE main.change_state SET state=?1,seq=?2 WHERE id=1",
        params![&after.state[..], after.seq as i64],
    )?;
    Ok(ApplyReceipt {
        before,
        after,
        rows: rows as u64,
        deletes: deletes as u64,
    })
}

/// Writes every bundled pack body to the replica's content file, in offset
/// order from the replica's committed end, then syncs; the rows that name
/// them commit afterwards in the same apply. Bytes past the end belong to no
/// row, so a failed apply leaves only an unreferenced tail.
fn append_content(c: &Connection, store: &Path) -> Result<(), ChangeError> {
    if !table_exists(c, "b", "pack")? {
        if table_exists(c, "b", "bundle_content")? {
            return Err(ChangeError::Integrity("bundle content without packs"));
        }
        return Ok(());
    }
    if !table_exists(c, "b", "bundle_content")? {
        return Err(ChangeError::Integrity("bundled packs without content"));
    }
    let unmatched: i64 = c.query_row(
        "SELECT (SELECT count(*) FROM b.bundle_content WHERE pack_id NOT IN (SELECT pack_id FROM b.pack)) \
         + (SELECT count(*) FROM b.pack WHERE pack_id NOT IN (SELECT pack_id FROM b.bundle_content))",
        [],
        |r| r.get(0),
    )?;
    if unmatched != 0 {
        return Err(ChangeError::Integrity(
            "bundle content does not match its packs",
        ));
    }
    let mut end: i64 = c.query_row(
        "SELECT coalesce(max(content_offset+length),0) FROM main.pack",
        [],
        |r| r.get(0),
    )?;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(crate::config::content_path(store))?;
    if (file.metadata()?.len() as i64) < end {
        return Err(ChangeError::Integrity("the replica content file is short"));
    }
    let mut packs = c.prepare(
        "SELECT p.content_offset,p.length,c.body FROM b.pack p JOIN b.bundle_content c USING(pack_id) \
         ORDER BY p.content_offset",
    )?;
    let mut cursor = packs.query([])?;
    while let Some(row) = cursor.next()? {
        let (offset, length): (i64, i64) = (row.get(0)?, row.get(1)?);
        let body = row
            .get_ref(2)?
            .as_blob()
            .map_err(|_| ChangeError::Integrity("bundle content is not a BLOB"))?;
        if offset != end || body.len() as i64 != length {
            return Err(ChangeError::Integrity(
                "bundle content does not continue the replica's content",
            ));
        }
        file.write_all_at(body, offset as u64)?;
        end += length;
    }
    file.sync_data()?;
    Ok(())
}

/// Commits `outcome`'s transaction or rolls it back, then checkpoints so the
/// closed file holds every change.
fn finish<T>(c: &Connection, outcome: Result<T, ChangeError>) -> Result<T, ChangeError> {
    let value = match outcome {
        Ok(value) => value,
        Err(error) => {
            let _ = c.execute_batch("ROLLBACK");
            return Err(error);
        }
    };
    if let Err(error) = c.execute_batch("COMMIT") {
        let _ = c.execute_batch("ROLLBACK");
        return Err(match error.sqlite_error_code() {
            Some(rusqlite::ErrorCode::ConstraintViolation) => {
                ChangeError::Integrity("a foreign key is unsatisfied at commit")
            }
            _ => error.into(),
        });
    }
    c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
    Ok(value)
}
