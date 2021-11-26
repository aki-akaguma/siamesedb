use siamesedb::filedb::FileDbParams;
use siamesedb::DbXxx;

fn main() -> Result<(), std::io::Error> {
    _test_a1()?;
    Ok(())
}
fn _test_a1() -> Result<(), std::io::Error> {
    let db_name = "target/tmp/testA5.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db
        .db_map_string_with_params(
            "some_map1",
            FileDbParams {
                /*
                dat_buf_size: 16 * 1024 * 1024,
                idx_buf_size: 16 * 1024 * 1024,
                */
                dat_buf_size: 512 * 1024,
                idx_buf_size: 512 * 1024,
                /*
                dat_buf_size: 512 * 1024,
                idx_buf_size: 64 * 1024,
                */
            },
        )
        .unwrap();
    //
    let mut kv_vec: Vec<(String, String)> = Vec::new();
    let mut i: i64 = 0;
    loop {
        i += 1;
        if i > 2_000_000 {
            break;
        }
        if i % 10_000 == 0 {
            db_map.bulk_put_string(&kv_vec)?;
            kv_vec.clear();
        }
        let bytes = i.to_le_bytes();
        let k = format!("key-{}.{}.{}", bytes[0], bytes[1], bytes[2]);
        let v = format!("val-{}", i);
        kv_vec.push((k, v));
    }
    if !kv_vec.is_empty() {
        db_map.bulk_put_string(&kv_vec)?;
    }
    db_map.flush()
}
