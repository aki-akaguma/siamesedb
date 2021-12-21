use siamesedb::filedb::{FileBufSizeParam, FileDbMapBytes, FileDbParams};
use siamesedb::{Bytes, DbXxx};
use std::str::FromStr;

const LOOP_MAX: i64 = 2_000_000;

fn main() -> Result<(), std::io::Error> {
    let db_name = "target/tmp/testA6.siamesedb";
    //
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "-c" => _test_create(db_name)?,
        "-w" => _test_write(db_name)?,
        "-r" => _test_read(db_name)?,
        _ => {
            eprintln!("[usage] {} {{-c|-w|-r}}", args[0]);
        }
    }
    Ok(())
}

fn conv_to_kv_string(ki: i64, _vi: i64) -> (Bytes, String) {
    let bytes = ki.to_le_bytes();
    //let k = format!("{}.{}", bytes[0], bytes[1]);
    //let k = format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3]);
    //let k = format!("key-{}.{}.{}", bytes[0], bytes[1], bytes[2]);
    let k = format!("key-{}.{}.{}", bytes[0], bytes[1], bytes[2]).repeat(2);
    let v = format!("value-{}", ki).repeat(4);
    //let v = format!("value-{}", ki);
    //let v = format!("{}", _vi);
    //let v = String::new();
    (k.into(), v)
}

fn _test_create(db_name: &str) -> Result<(), std::io::Error> {
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db
        .db_map_bytes_with_params(
            "some_map1",
            FileDbParams {
                /*
                dat_buf_size: FileBufSizeParam::PerMille(100),
                idx_buf_size: FileBufSizeParam::PerMille(300),
                */
                dat_buf_size: FileBufSizeParam::PerMille(1000),
                idx_buf_size: FileBufSizeParam::PerMille(1000),
                /*
                dat_buf_size: FileBufSizeParam::Auto,
                idx_buf_size: FileBufSizeParam::Auto,
                */
            },
        )
        .unwrap();
    //
    db_map.read_fill_buffer()?;
    //
    let vi: i64 = 0;
    //
    let mut kv_vec: Vec<(Bytes, String)> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % 10_000 == 0 {
            db_map.bulk_put_string(&kv_vec)?;
            kv_vec.clear();
        }
        let (k, v) = conv_to_kv_string(ki, vi);
        kv_vec.push((k, v));
    }
    if !kv_vec.is_empty() {
        db_map.bulk_put_string(&kv_vec)?;
    }
    db_map.flush()
}

fn _test_write(db_name: &str) -> Result<(), std::io::Error> {
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db
        .db_map_bytes_with_params(
            "some_map1",
            FileDbParams {
                /*
                dat_buf_size: FileBufSizeParam::PerMille(100),
                idx_buf_size: FileBufSizeParam::PerMille(300),
                */
                dat_buf_size: FileBufSizeParam::PerMille(1000),
                idx_buf_size: FileBufSizeParam::PerMille(1000),
                /*
                dat_buf_size: FileBufSizeParam::Auto,
                idx_buf_size: FileBufSizeParam::Auto,
                */
            },
        )
        .unwrap();
    //
    db_map.read_fill_buffer()?;
    //
    let (k, _v) = conv_to_kv_string(1, 0);
    let vi: i64 = {
        if let Some(s) = db_map.get_string(&k)? {
            match i64::from_str(&s) {
                Ok(i) => i + 1,
                Err(_) => 0,
            }
        } else {
            0
        }
    };
    //
    let mut kv_vec: Vec<(Bytes, String)> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % 10_000 == 0 {
            db_map.bulk_put_string(&kv_vec)?;
            kv_vec.clear();
        }
        let (k, v) = conv_to_kv_string(ki, vi);
        kv_vec.push((k, v));
    }
    if !kv_vec.is_empty() {
        db_map.bulk_put_string(&kv_vec)?;
    }
    db_map.flush()
}

fn _test_read(db_name: &str) -> Result<(), std::io::Error> {
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db
        .db_map_bytes_with_params(
            "some_map1",
            FileDbParams {
                /*
                dat_buf_size: FileBufSizeParam::PerMille(333),
                idx_buf_size: FileBufSizeParam::PerMille(430),
                 */
                dat_buf_size: FileBufSizeParam::PerMille(1000),
                idx_buf_size: FileBufSizeParam::PerMille(1000),
                /*
                dat_buf_size: FileBufSizeParam::Auto,
                idx_buf_size: FileBufSizeParam::Auto,
                */
            },
        )
        .unwrap();
    //
    db_map.read_fill_buffer()?;
    //
    let (k, _v) = conv_to_kv_string(1, 0);
    let vi: i64 = {
        if let Some(s) = db_map.get_string(&k)? {
            i64::from_str(&s).unwrap_or(0)
        } else {
            0
        }
    };
    //
    let mut key_vec: Vec<Bytes> = Vec::new();
    let mut value_vec: Vec<String> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % 10_000 == 0 {
            _test_read_one(&mut db_map, &key_vec, &value_vec)?;
            //
            key_vec.clear();
            value_vec.clear();
        }
        let (k, correct) = conv_to_kv_string(ki, vi);
        key_vec.push(k);
        value_vec.push(correct);
    }
    if !key_vec.is_empty() {
        _test_read_one(&mut db_map, &key_vec, &value_vec)?;
        //
        key_vec.clear();
        value_vec.clear();
    }
    Ok(())
}

fn _test_read_one(
    db_map: &mut FileDbMapBytes,
    key_vec: &[Bytes],
    value_vec: &[String],
) -> Result<(), std::io::Error> {
    let keys: Vec<&Bytes> = key_vec.iter().collect();
    let result = db_map.bulk_get_string(&keys)?;
    //
    for (idx, answer) in result.iter().enumerate() {
        if let Some(answer) = answer {
            let correct = &value_vec[idx];
            if answer != correct {
                panic!("invalid value: {:?} != {:?}", answer, correct);
            }
        } else {
            panic!("not found value: {} => {}", key_vec[idx], value_vec[idx]);
        }
    }
    Ok(())
}