mod test {
    use siamesedb::{Bytes, DbXxx};
    //
    fn basic_test_map_string<T: DbXxx<String>>(db_map: &mut T) {
        // get nothing
        let r = db_map.get_string("key1").unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string("key1".to_string(), "value1").unwrap();
        db_map.put_string("key2".to_string(), "value2").unwrap();
        db_map.put_string("key3".to_string(), "value3").unwrap();
        db_map.put_string("key4".to_string(), "value4").unwrap();
        db_map.put_string("key5".to_string(), "value5").unwrap();
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
        //
        db_map.sync_data().unwrap();
    }
    fn basic_test_map_u64<T: DbXxx<u64>>(db_map: &mut T) {
        // get nothing
        let r = db_map.get_string(&1023).unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(1021, "value1").unwrap();
        db_map.put_string(1022, "value2").unwrap();
        db_map.put_string(1023, "value3").unwrap();
        db_map.put_string(1024, "value4").unwrap();
        db_map.put_string(1025, "value5").unwrap();
        // get hits
        let r = db_map.get_string(&1021).unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_map.get_string(&1023).unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_map.get_string(&1025).unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_map.put_string(1023, "VALUEVALUE3").unwrap();
        let r = db_map.get_string(&1023).unwrap();
        assert_eq!(r, Some("VALUEVALUE3".to_string()));
        // delete
        db_map.delete(&1023).unwrap();
        let r = db_map.get_string(&1023).unwrap();
        assert_eq!(r, None);
        //
        db_map.sync_data().unwrap();
    }
    fn basic_test_map_bytes<T: DbXxx<Bytes>>(db_map: &mut T) {
        // get nothing
        let r = db_map.get_string(&b"key1".into()).unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(b"key1".into(), "value1").unwrap();
        db_map.put_string(b"key2".into(), "value2").unwrap();
        db_map.put_string(b"key3".into(), "value3").unwrap();
        db_map.put_string(b"key4".into(), "value4").unwrap();
        db_map.put_string(b"key5".into(), "value5").unwrap();
        // get hits
        let r = db_map.get_string(&b"key1".into()).unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_map.get_string(&b"key3".into()).unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_map.get_string(&b"key5".into()).unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_map.put_string(b"key3".into(), "VALUEVALUE3").unwrap();
        let r = db_map.get_string(&b"key3".into()).unwrap();
        assert_eq!(r, Some("VALUEVALUE3".to_string()));
        // delete
        db_map.delete(&b"key3".into()).unwrap();
        let r = db_map.get_string(&b"key3".into()).unwrap();
        assert_eq!(r, None);
        //
        // large data
        let key = &"key9".repeat(25);
        let val = &"value8".repeat(70);
        let val2 = &"value9".repeat(70);
        // insert
        db_map.put_string(key.into(), val).unwrap();
        // get hits
        let r = db_map.get_string(&key.into()).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // delete
        db_map.delete(&key.into()).unwrap();
        let r = db_map.get_string(&key.into()).unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(key.into(), val2).unwrap();
        // get hits
        let r = db_map.get_string(&key.into()).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(&key.into()).unwrap();
        let r = db_map.get_string(&key.into()).unwrap();
        assert_eq!(r, None);
        //
        // special case
        // get empty key
        let r = db_map.get_string(&b"".into()).unwrap();
        assert_eq!(r, None);
        // insert empty key
        db_map.put_string(b"".into(), val2).unwrap();
        // get empty key
        let r = db_map.get_string(&b"".into()).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_string<T: DbXxx<String>>(db_map: &mut T) {
        let key = "The Adventure of the Missing Three-Quarter";
        let val = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years";
        let val2 = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years ago and gave Mr.
     Sherlock Holmes a puzzled quarter of an hour.";
        // put
        db_map.put_string(key.to_string(), val).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // modify
        db_map.put_string(key.to_string(), val2).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
    }
    fn medium_test_map_u64<T: DbXxx<u64>>(db_map: &mut T) {
        let key = 123456789;
        let val = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years";
        let val2 = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years ago and gave Mr.
     Sherlock Holmes a puzzled quarter of an hour.";
        db_map.put_string(key, val).unwrap();
        // get hits
        let r = db_map.get_string(&key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // modify
        db_map.put_string(key, val2).unwrap();
        let r = db_map.get_string(&key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(&key).unwrap();
        let r = db_map.get_string(&key).unwrap();
        assert_eq!(r, None);
    }
    fn medium_test_map_bytes<T: DbXxx<Bytes>>(db_map: &mut T) {
        let key = b"The Adventure of the Missing Three-Quarter";
        let val = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years";
        let val2 = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years ago and gave Mr.
     Sherlock Holmes a puzzled quarter of an hour.";
        // put
        db_map.put_string(key.into(), val).unwrap();
        // get hits
        let r = db_map.get_string(&key.into()).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // modify
        db_map.put_string(key.into(), val2).unwrap();
        let r = db_map.get_string(&key.into()).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(&key.into()).unwrap();
        let r = db_map.get_string(&key.into()).unwrap();
        assert_eq!(r, None);
    }
    ////
    #[test]
    fn test_memory_map_string() {
        let db = siamesedb::open_memory();
        let mut db_map = db.db_map_string("some_string_1");
        basic_test_map_string(&mut db_map);
        medium_test_map_string(&mut db_map);
    }
    #[test]
    fn test_memory_map_u64() {
        let db = siamesedb::open_memory();
        let mut db_map = db.db_map_u64("some_u64_1");
        basic_test_map_u64(&mut db_map);
        medium_test_map_u64(&mut db_map);
    }
    #[test]
    fn test_memory_map_bytes() {
        let db = siamesedb::open_memory();
        let mut db_map = db.db_map_bytes("some_bytes_1");
        basic_test_map_bytes(&mut db_map);
        medium_test_map_bytes(&mut db_map);
    }
    ////
    #[test]
    fn test_file_map_string() {
        let db_name = "target/tmp/test1-s.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_string("some_string_1").unwrap();
        basic_test_map_string(&mut db_map);
        medium_test_map_string(&mut db_map);
    }
    #[test]
    fn test_file_map_u64() {
        let db_name = "target/tmp/test1-u.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_u64("some_u64_1").unwrap();
        basic_test_map_u64(&mut db_map);
        medium_test_map_u64(&mut db_map);
    }
    #[test]
    fn test_file_map_bytes() {
        let db_name = "target/tmp/test1-b.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_bytes("some_bytes_1").unwrap();
        basic_test_map_bytes(&mut db_map);
        medium_test_map_bytes(&mut db_map);
    }
    //
    /*
    proptest! {
        #[test]
        fn proptest_file_map(s in ) {
            let db_name = "target/tmp/test1.shamdb";
            let _ = std::fs::remove_dir_all(db_name);
            let db = shamdb::open_file(db_name).unwrap();
            let mut db_map = db.db_map("some_map1").unwrap();
        }
    }
    */
}
