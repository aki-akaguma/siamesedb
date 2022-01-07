use siamesedb::filedb::{FileBufSizeParam, FileDbMapString, FileDbParams};
use siamesedb::{DbString, DbXxx, DbXxxKeyType};
use std::str::FromStr;

const LOOP_MAX: i64 = 2_000_000;
const BULK_COUNT: i64 = 10_000;

fn main() -> Result<(), std::io::Error> {
    let db_name = "target/tmp/testA5.siamesedb";
    //
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "-c" => _test_create(db_name)?,
        "-w" => _test_write(db_name)?,
        "-r" => _test_read(db_name)?,
        "-d" => _test_delete(db_name)?,
        _ => {
            eprintln!("[usage] {} {{-c|-w|-r|-d}}", args[0]);
        }
    }
    Ok(())
}

fn open_db_map(db_name: &str) -> Result<FileDbMapString, std::io::Error> {
    let db = siamesedb::open_file(db_name).unwrap();
    db.db_map_string_with_params(
        "some_map1",
        FileDbParams {
            key_buf_size: FileBufSizeParam::PerMille(1000),
            idx_buf_size: FileBufSizeParam::PerMille(1000),
            htx_buf_size: FileBufSizeParam::PerMille(1000),
            /*
            key_buf_size: FileBufSizeParam::PerMille(100),
            idx_buf_size: FileBufSizeParam::PerMille(300),
            key_buf_size: FileBufSizeParam::Auto,
            idx_buf_size: FileBufSizeParam::Auto,
            */
            ..Default::default()
        },
    )
}

fn conv_to_kv_string(ki: i64, _vi: i64) -> (DbString, String) {
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
    _test_write(db_name)
}

fn _test_write(db_name: &str) -> Result<(), std::io::Error> {
    let mut db_map = open_db_map(db_name)?;
    db_map.read_fill_buffer()?;
    //
    let (k, _v) = conv_to_kv_string(1, 0);
    let vi: i64 = {
        if let Some(s) = db_map.get_string(k.as_bytes())? {
            match i64::from_str(&s) {
                Ok(i) => i + 1,
                Err(_) => 0,
            }
        } else {
            0
        }
    };
    //
    let mut kv_vec: Vec<(DbString, String)> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % BULK_COUNT == 0 {
            #[cfg(feature = "htx")]
            _test_write_one(&mut db_map, &kv_vec)?;
            #[cfg(not(feature = "htx"))]
            db_map.bulk_put_string(&kv_vec)?;
            kv_vec.clear();
        }
        let (k, v) = conv_to_kv_string(ki, vi);
        kv_vec.push((k, v));
    }
    if !kv_vec.is_empty() {
        #[cfg(feature = "htx")]
        _test_write_one(&mut db_map, &kv_vec)?;
        #[cfg(not(feature = "htx"))]
        db_map.bulk_put_string(&kv_vec)?;
    }
    db_map.flush()
}

#[cfg(feature = "htx")]
fn _test_write_one(
    db_map: &mut FileDbMapString,
    key_vec: &[(DbString, String)],
) -> Result<(), std::io::Error> {
    let keys: Vec<(DbString, &[u8])> = key_vec
        .iter()
        .map(|(a, b)| (a.clone(), b.as_bytes()))
        .collect();
    db_map.bulk_put(&keys)
}

fn _test_read(db_name: &str) -> Result<(), std::io::Error> {
    let mut db_map = open_db_map(db_name)?;
    db_map.read_fill_buffer()?;
    //
    let (k, _v) = conv_to_kv_string(1, 0);
    let vi: i64 = {
        if let Some(s) = db_map.get_string(k.as_bytes())? {
            i64::from_str(&s).unwrap_or(0)
        } else {
            0
        }
    };
    //
    let mut key_vec: Vec<DbString> = Vec::new();
    let mut value_vec: Vec<String> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % BULK_COUNT == 0 {
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
    db_map: &mut FileDbMapString,
    key_vec: &[DbString],
    value_vec: &[String],
) -> Result<(), std::io::Error> {
    let keys: Vec<&DbString> = key_vec.iter().collect();
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

fn _test_delete(db_name: &str) -> Result<(), std::io::Error> {
    let mut db_map = open_db_map(db_name)?;
    db_map.read_fill_buffer()?;
    //
    let (k, _v) = conv_to_kv_string(1, 0);
    let vi: i64 = {
        if let Some(s) = db_map.get_string(k.as_bytes())? {
            i64::from_str(&s).unwrap_or(0)
        } else {
            0
        }
    };
    //
    let mut key_vec: Vec<DbString> = Vec::new();
    let mut value_vec: Vec<String> = Vec::new();
    let mut ki: i64 = 0;
    loop {
        ki += 1;
        if ki > LOOP_MAX {
            break;
        }
        if ki % BULK_COUNT == 0 {
            _test_delete_one(&mut db_map, &key_vec, &value_vec)?;
            //
            key_vec.clear();
            value_vec.clear();
        }
        let (k, correct) = conv_to_kv_string(ki, vi);
        key_vec.push(k);
        value_vec.push(correct);
    }
    if !key_vec.is_empty() {
        _test_delete_one(&mut db_map, &key_vec, &value_vec)?;
        //
        key_vec.clear();
        value_vec.clear();
    }
    Ok(())
}

fn _test_delete_one(
    db_map: &mut FileDbMapString,
    key_vec: &[DbString],
    value_vec: &[String],
) -> Result<(), std::io::Error> {
    let keys: Vec<&DbString> = key_vec.iter().collect();
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
