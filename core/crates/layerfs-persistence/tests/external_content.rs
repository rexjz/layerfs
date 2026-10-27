//! `ExternalContent` (schema 19): pack metadata rows in SQLite, every body
//! appended in publication order to `<store>-content`. Reads return the same
//! canonical bytes and complete packs as the monolithic layout; the content
//! file only grows, a crashed tail is overwritten and trimmed at seal, and a
//! changed or missing byte below the committed end is refused.
mod support;
use layerfs_content::{FinalizedObject, ObjectRole};
use layerfs_history::HistoryCatalogConfig;
use layerfs_persistence::{
    content_path, Handles, PersistenceConfig, SqlitePackLayout, SqlitePersistenceProfile,
};
use layerfs_storage::{
    location::{ObjectLocation, PackDomain, PackInfo},
    pack::{assemble, build_group, layout::PackLane},
    port::*,
    Storage, StoragePolicy,
};
use std::{io::Write, os::unix::fs::FileExt, sync::Arc};
fn config() -> HistoryCatalogConfig {
    HistoryCatalogConfig {
        binding_key: b"external-content".to_vec(),
        cursor_key: [74; 32],
        incarnation: 1,
    }
}
fn persistence(path: &std::path::Path) -> PersistenceConfig {
    PersistenceConfig::sqlite(path)
        .with_sqlite_profile(SqlitePersistenceProfile::Disposable)
        .with_sqlite_pack_layout(SqlitePackLayout::ExternalContent)
}
fn create(path: &std::path::Path) -> Handles {
    Handles::create(
        persistence(path),
        StoragePolicy::frozen_default(),
        &config(),
    )
    .unwrap()
}
fn reopen(path: &std::path::Path) -> Result<Handles, PersistenceError> {
    Handles::open_writable(
        PersistenceConfig::sqlite(path).with_sqlite_profile(SqlitePersistenceProfile::Disposable),
        &config().binding_key,
        config().cursor_key,
    )
}
/// Publishes pack `pack_id` holding eight random 9,000-byte objects.
fn publish(h: &Handles, pack_id: i64, seed: u64) -> (Vec<FinalizedObject>, usize) {
    let mut random = seed;
    let objects = (0..8)
        .map(|_| {
            let bytes = (0..9000)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    random as u8
                })
                .collect::<Vec<_>>();
            FinalizedObject::new(
                ObjectRole::FileState,
                layerfs_content::object::codec::encode_bytes_object(&bytes).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let groups = objects
        .iter()
        .map(|o| {
            let mut record = vec![0];
            record.extend_from_slice(o.canonical());
            build_group(PackLane::Ordinary, &[record], None).unwrap()
        })
        .collect::<Vec<_>>();
    let body = assemble(PackLane::Ordinary, &groups).unwrap();
    let length = body.len();
    h.storage
        .publish(&Publication {
            packs: vec![PublishedPack {
                info: PackInfo {
                    pack_id,
                    domain: PackDomain::Metadata,
                    key: ObjectKey::for_bytes(&body),
                    length,
                },
                body: Arc::new(body),
            }],
            objects: objects
                .iter()
                .enumerate()
                .map(|(n, o)| ObjectLocation {
                    object_id: o.id(),
                    role: o.role(),
                    canonical_length: o.canonical_len(),
                    pack_id,
                    group_number: n,
                    record_number: 0,
                })
                .collect(),
            ..Publication::default()
        })
        .unwrap();
    (objects, length)
}
fn read(h: &Handles, objects: &[&FinalizedObject]) -> Result<Vec<Vec<u8>>, String> {
    Storage::new(h.storage.clone())
        .unwrap()
        .reader()
        .unwrap()
        .read_objects(&objects.iter().map(|o| o.id()).collect::<Vec<_>>())
        .map_err(|error| format!("{error:?}"))
}
fn content_len(path: &std::path::Path) -> u64 {
    std::fs::metadata(content_path(path)).unwrap().len()
}
#[test]
fn bodies_live_in_the_content_file_and_read_back_exactly() {
    let t = support::Temp::new("external-read");
    let path = t.join("db");
    let h = create(&path);
    assert_eq!(h.profile().pack_layout, SqlitePackLayout::ExternalContent);
    let (objects, length) = publish(&h, 1, 931823);
    let db = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        19
    );
    assert!(db.prepare("SELECT body FROM pack").is_err());
    assert_eq!(
        db.query_row("SELECT content_offset,length FROM pack", [], |r| Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?
        )))
        .unwrap(),
        (0, length as i64)
    );
    drop(db);
    assert_eq!(content_len(&path), length as u64);
    let before = h.diagnostics().unwrap();
    assert_eq!(
        read(&h, &[&objects[7], &objects[1], &objects[7]]).unwrap(),
        vec![
            objects[7].canonical().to_vec(),
            objects[1].canonical().to_vec(),
            objects[7].canonical().to_vec()
        ]
    );
    let work = h.diagnostics().unwrap();
    // Only the selected groups are read, not the whole 72 KB pack.
    assert!(work.blob_read_bytes - before.blob_read_bytes < 30000);
    let mut out = Vec::new();
    h.storage.read_packs(&[1], &mut out).unwrap();
    assert_eq!(out[0].info().length, length);
    drop(h);
    let reopened = Handles::open_read_only(
        PersistenceConfig::sqlite(&path).with_sqlite_profile(SqlitePersistenceProfile::Disposable),
        &config().binding_key,
        config().cursor_key,
    )
    .unwrap();
    assert_eq!(
        read(&reopened, &[&objects[0]]).unwrap(),
        vec![objects[0].canonical().to_vec()]
    );
}
#[test]
fn publications_append_contiguously_and_earlier_bytes_never_change() {
    let t = support::Temp::new("external-append");
    let path = t.join("db");
    let h = create(&path);
    let (first, a) = publish(&h, 1, 11);
    let prefix = std::fs::read(content_path(&path)).unwrap();
    let (second, b) = publish(&h, 2, 22);
    let (third, c) = publish(&h, 3, 33);
    assert_eq!(content_len(&path), (a + b + c) as u64);
    let file = std::fs::read(content_path(&path)).unwrap();
    assert_eq!(&file[..a], prefix.as_slice());
    let db = rusqlite::Connection::open(&path).unwrap();
    let offsets: Vec<i64> = db
        .prepare("SELECT content_offset FROM pack ORDER BY pack_id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(offsets, vec![0, a as i64, (a + b) as i64]);
    drop(db);
    assert_eq!(
        read(&h, &[&first[3], &second[4], &third[5]]).unwrap(),
        vec![
            first[3].canonical().to_vec(),
            second[4].canonical().to_vec(),
            third[5].canonical().to_vec()
        ]
    );
}
#[test]
fn a_crashed_tail_is_overwritten_then_trimmed_at_seal() {
    let t = support::Temp::new("external-tail");
    let path = t.join("db");
    let h = create(&path);
    let (first, a) = publish(&h, 1, 44);
    // A publication that wrote its bodies but never committed its rows.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(content_path(&path))
        .unwrap();
    file.write_all(&vec![0xee; 200_000]).unwrap();
    drop(file);
    let (second, b) = publish(&h, 2, 55);
    assert_eq!(content_len(&path), (a + 200_000) as u64);
    assert_eq!(
        read(&h, &[&first[0], &second[7]]).unwrap(),
        vec![
            first[0].canonical().to_vec(),
            second[7].canonical().to_vec()
        ]
    );
    let sealed = h.seal().unwrap();
    assert_eq!(sealed.content_bytes, Some((a + b) as u64));
    assert_eq!(content_len(&path), (a + b) as u64);
    let reopened = reopen(&path).unwrap();
    assert_eq!(
        read(&reopened, &[&second[0]]).unwrap(),
        vec![second[0].canonical().to_vec()]
    );
}
#[test]
fn a_changed_content_byte_is_refused_for_its_pack_only() {
    let t = support::Temp::new("external-corrupt");
    let path = t.join("db");
    let h = create(&path);
    let (first, _) = publish(&h, 1, 66);
    let (second, _) = publish(&h, 2, 77);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(content_path(&path))
        .unwrap();
    // A byte inside the second pack's last group.
    let mut byte = [0];
    let at = content_len(&path) - 100;
    file.read_exact_at(&mut byte, at).unwrap();
    file.write_all_at(&[byte[0] ^ 0x5a], at).unwrap();
    drop(file);
    assert_eq!(
        read(&h, &[&first[7]]).unwrap(),
        vec![first[7].canonical().to_vec()]
    );
    assert!(read(&h, &[&second[7]]).is_err());
    let mut out = Vec::new();
    assert!(h.storage.read_packs(&[2], &mut out).is_err());
}
#[test]
fn a_content_file_shorter_than_the_committed_end_is_refused_at_open() {
    let t = support::Temp::new("external-short");
    let path = t.join("db");
    let h = create(&path);
    let (_, a) = publish(&h, 1, 88);
    drop(h);
    std::fs::OpenOptions::new()
        .write(true)
        .open(content_path(&path))
        .unwrap()
        .set_len(a as u64 - 1)
        .unwrap();
    assert!(reopen(&path).is_err());
    std::fs::remove_file(content_path(&path)).unwrap();
    assert!(reopen(&path).is_err());
}
#[test]
fn the_schema_refuses_a_pack_that_does_not_start_at_the_end() {
    let t = support::Temp::new("external-trigger");
    let path = t.join("db");
    let h = create(&path);
    let (_, a) = publish(&h, 1, 99);
    drop(h);
    let db = rusqlite::Connection::open(&path).unwrap();
    let insert = |offset: i64| {
        db.execute(
            "INSERT INTO pack(pack_id,domain,digest,length,content_offset) VALUES(9,0,zeroblob(32),64,?1)",
            [offset],
        )
    };
    assert!(insert(a as i64 + 1).is_err());
    assert!(insert(0).is_err());
    assert!(insert(a as i64).is_ok());
}
#[test]
fn one_publication_of_several_packs_appends_them_in_order() {
    let t = support::Temp::new("external-batch");
    let path = t.join("db");
    let h = create(&path);
    let bodies = [PackLane::Ordinary, PackLane::Chunk, PackLane::WholeFile]
        .into_iter()
        .enumerate()
        .map(|(n, lane)| {
            let body = assemble(
                lane,
                &[build_group(lane, &[vec![0x10 + n as u8; 6400 * (n + 1)]], None).unwrap()],
            )
            .unwrap();
            PublishedPack {
                info: PackInfo {
                    pack_id: n as i64 + 1,
                    domain: PackDomain::for_lane(lane),
                    key: ObjectKey::for_bytes(&body),
                    length: body.len(),
                },
                body: Arc::new(body),
            }
        })
        .collect::<Vec<_>>();
    h.storage
        .publish(&Publication {
            packs: bodies.clone(),
            ..Publication::default()
        })
        .unwrap();
    let total: usize = bodies.iter().map(|p| p.body.len()).sum();
    assert_eq!(content_len(&path), total as u64);
    for pack in &bodies {
        let mut out = Vec::new();
        h.storage
            .read_packs(&[pack.info.pack_id], &mut out)
            .unwrap();
        assert_eq!(out[0].info(), pack.info);
        assert_eq!(out[0].body(), pack.body.as_slice());
    }
}
