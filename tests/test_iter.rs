mod test_iter {
    use siamesedb::{DbBytes, DbInt, DbMap, DbString};
    //
    fn basic_test_map_string<T: DbMap<DbString>>(db_map: &mut T) {
        // insert
        db_map.put_string("key01", "value1").unwrap();
        db_map.put_string("key02", "value2").unwrap();
        db_map.put_string("key03", "value3").unwrap();
        db_map.put_string("key04", "value4").unwrap();
        db_map.put_string("key05", "value5").unwrap();
        // iterator
        let mut iter = db_map.iter_mut();
        assert_eq!(iter.next(), Some(("key01".into(), "value1".into())));
        assert_eq!(iter.next(), Some(("key02".into(), "value2".into())));
        assert_eq!(iter.next(), Some(("key03".into(), "value3".into())));
        assert_eq!(iter.next(), Some(("key04".into(), "value4".into())));
        assert_eq!(iter.next(), Some(("key05".into(), "value5".into())));
        assert_eq!(iter.next(), None);
        /*
        // get hits
        let r = db_map.get_string("key1").unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_map.get_string("key5").unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_map
            .put_string("key3".to_string(), "VALUEVALUE3")
            .unwrap();
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, Some("VALUEVALUE3".to_string()));
        // delete
        db_map.delete("key3").unwrap();
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, None);
        //
        // large data
        let key = &"key9".repeat(25);
        let val = &"value8".repeat(70);
        let val2 = &"value9".repeat(70);
        // insert
        db_map.put_string(key.to_string(), val).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(key.to_string(), val2).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
        //
        // special case
        // get empty key
        let r = db_map.get_string("").unwrap();
        assert_eq!(r, None);
        // insert empty key
        db_map.put_string("".to_string(), val2).unwrap();
        // get empty key
        let r = db_map.get_string("").unwrap();
        assert_eq!(r, Some(val2.to_string()));
        */
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_string<T: DbMap<DbString>>(db_map: &mut T) {
        #[rustfmt::skip]
        const LOOP_MAX: i32 = if cfg!(miri) { 10 } else { 100 };
        // insert
        for i in 0..LOOP_MAX {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            db_map.put_string(&key, &value).unwrap();
        }
        // iterator
        let mut iter = db_map.iter_mut();
        for i in 0..LOOP_MAX {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(iter.next(), Some((key.into(), value.as_bytes().to_vec())));
        }
        assert_eq!(iter.next(), None);
        //
        // iter on loop
        for (i, (k, v)) in (0_i32..).zip(db_map.iter()) {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(k, key.into());
            assert_eq!(v, value.as_bytes().to_vec());
        }
        //
        // into iter on loop
        //let mut iter = db_map.into_iter();
        /*
        let mut i: i32 = 0;
        for (k, v) in db_map {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(k, key);
            assert_eq!(v, value.as_bytes().to_vec());
            i += 1;
        }
        */
        //
        //db_map.sync_data().unwrap();
    }
    fn basic_test_map_dbint<T: DbMap<DbInt>>(db_map: &mut T) {
        // insert
        db_map.put_string(&12301, "value1").unwrap();
        db_map.put_string(&12302, "value2").unwrap();
        db_map.put_string(&12303, "value3").unwrap();
        db_map.put_string(&12304, "value4").unwrap();
        db_map.put_string(&12305, "value5").unwrap();
        // iterator
        let mut iter = db_map.iter_mut();
        assert_eq!(iter.next(), Some((12301.into(), b"value1".to_vec())));
        assert_eq!(iter.next(), Some((12302.into(), b"value2".to_vec())));
        assert_eq!(iter.next(), Some((12303.into(), b"value3".to_vec())));
        assert_eq!(iter.next(), Some((12304.into(), b"value4".to_vec())));
        assert_eq!(iter.next(), Some((12305.into(), b"value5".to_vec())));
        assert_eq!(iter.next(), None);
        /*
        // get hits
        let r = db_map.get_string("key1").unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_map.get_string("key5").unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_map
            .put_string("key3".to_string(), "VALUEVALUE3")
            .unwrap();
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, Some("VALUEVALUE3".to_string()));
        // delete
        db_map.delete("key3").unwrap();
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, None);
        //
        // large data
        let key = &"key9".repeat(25);
        let val = &"value8".repeat(70);
        let val2 = &"value9".repeat(70);
        // insert
        db_map.put_string(key.to_string(), val).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(key.to_string(), val2).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
        //
        // special case
        // get empty key
        let r = db_map.get_string("").unwrap();
        assert_eq!(r, None);
        // insert empty key
        db_map.put_string("".to_string(), val2).unwrap();
        // get empty key
        let r = db_map.get_string("").unwrap();
        assert_eq!(r, Some(val2.to_string()));
        */
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_dbint<T: DbMap<DbInt>>(db_map: &mut T) {
        #[rustfmt::skip]
        const LOOP_MAX: i32 = if cfg!(miri) { 10 } else { 100 };
        // insert
        for i in 0..LOOP_MAX {
            let key = 12300u64 + i as u64;
            let value = format!("value{}", i);
            db_map.put_string(&key, &value).unwrap();
        }
        // iterator
        let mut iter = db_map.iter_mut();
        for i in 0..LOOP_MAX {
            let key = 12300u64 + i as u64;
            let value = format!("value{}", i);
            assert_eq!(iter.next(), Some((key.into(), value.as_bytes().to_vec())));
        }
        assert_eq!(iter.next(), None);
        //
        // iter on loop
        for (i, (k, v)) in (0_i32..).zip(db_map.iter()) {
            let key = 12300u64 + i as u64;
            let value = format!("value{}", i);
            assert_eq!(k, key.into());
            assert_eq!(v, value.as_bytes().to_vec());
        }
        //
        // into iter on loop
        //let mut iter = db_map.into_iter();
        /*
        let mut i: i32 = 0;
        for (k, v) in db_map {
            let key = 12300 + i;
            let value = format!("value{}", i);
            assert_eq!(k, key);
            assert_eq!(v, value.as_bytes().to_vec());
            i += 1;
        }
        */
        //
        //db_map.sync_data().unwrap();
    }
    fn basic_test_map_bytes<T: DbMap<DbBytes>>(db_map: &mut T) {
        // insert
        db_map.put_string(b"key01", "value1").unwrap();
        db_map.put_string(b"key02", "value2").unwrap();
        db_map.put_string(b"key03", "value3").unwrap();
        db_map.put_string(b"key04", "value4").unwrap();
        db_map.put_string(b"key05", "value5").unwrap();
        // iterator
        let mut iter = db_map.iter_mut();
        assert_eq!(iter.next(), Some((b"key01".into(), b"value1".to_vec())));
        assert_eq!(iter.next(), Some((b"key02".into(), b"value2".to_vec())));
        assert_eq!(iter.next(), Some((b"key03".into(), b"value3".to_vec())));
        assert_eq!(iter.next(), Some((b"key04".into(), b"value4".to_vec())));
        assert_eq!(iter.next(), Some((b"key05".into(), b"value5".to_vec())));
        assert_eq!(iter.next(), None);
        /*
        // get hits
        let r = db_map.get_string("key1").unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_map.get_string("key5").unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_map
            .put_string("key3".to_string(), "VALUEVALUE3")
            .unwrap();
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, Some("VALUEVALUE3".to_string()));
        // delete
        db_map.delete("key3").unwrap();
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, None);
        //
        // large data
        let key = &"key9".repeat(25);
        let val = &"value8".repeat(70);
        let val2 = &"value9".repeat(70);
        // insert
        db_map.put_string(key.to_string(), val).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(key.to_string(), val2).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
        //
        // special case
        // get empty key
        let r = db_map.get_string("").unwrap();
        assert_eq!(r, None);
        // insert empty key
        db_map.put_string("".to_string(), val2).unwrap();
        // get empty key
        let r = db_map.get_string("").unwrap();
        assert_eq!(r, Some(val2.to_string()));
        */
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_bytes<T: DbMap<DbBytes>>(db_map: &mut T) {
        #[rustfmt::skip]
        const LOOP_MAX: i32 = if cfg!(miri) { 10 } else { 100 };
        // insert
        for i in 0..LOOP_MAX {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            db_map.put_string(&key, &value).unwrap();
        }
        // iterator
        let mut iter = db_map.iter_mut();
        for i in 0..LOOP_MAX {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(iter.next(), Some((key.into(), value.as_bytes().to_vec())));
        }
        assert_eq!(iter.next(), None);
        //
        // iter on loop
        for (i, (k, v)) in (0_i32..).zip(db_map.iter()) {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(k, key.into());
            assert_eq!(v, value.as_bytes().to_vec());
        }
        //
        // into iter on loop
        //let mut iter = db_map.into_iter();
        /*
        let mut i: i32 = 0;
        for (k, v) in db_map {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(k, key);
            assert_eq!(v, value.as_bytes().to_vec());
            i += 1;
        }
        */
        //
        //db_map.sync_data().unwrap();
    }
    //
    #[test]
    fn test_file_map_string() {
        let db_name = "target/tmp/test_iter-s.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_string("some_string_1").unwrap();
        basic_test_map_string(&mut db_map);
        medium_test_map_string(&mut db_map);
    }
    #[test]
    fn test_file_map_dbint() {
        let db_name = "target/tmp/test_iter-u.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_int("some_u64_1").unwrap();
        basic_test_map_dbint(&mut db_map);
        medium_test_map_dbint(&mut db_map);
    }
    #[test]
    fn test_file_map_bytes() {
        let db_name = "target/tmp/test_iter-b.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_bytes("some_bytes_1").unwrap();
        basic_test_map_bytes(&mut db_map);
        medium_test_map_bytes(&mut db_map);
    }
}
