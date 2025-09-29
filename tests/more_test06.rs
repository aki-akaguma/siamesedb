mod test {
    use siamesedb::{DbBytes, DbXxx};
    //
    fn basic_test_map_bytes<T: DbXxx<DbBytes>>(db_map: &mut T) {
        // get nothing
        let r = db_map.get_string(b"key1").unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(b"key1", "value1").unwrap();
        db_map.put_string(b"key2", "value2").unwrap();
        db_map.put_string(b"key3", "value3").unwrap();
        db_map.put_string(b"key4", "value4").unwrap();
        db_map.put_string(b"key5", "value5").unwrap();
        // get hits
        let r = db_map.get_string(b"key1").unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_map.get_string(b"key3").unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_map.get_string(b"key5").unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_map.put_string(b"key3", "VALUEVALUE3").unwrap();
        let r = db_map.get_string(b"key3").unwrap();
        assert_eq!(r, Some("VALUEVALUE3".to_string()));
        // delete
        db_map.delete(b"key3").unwrap();
        let r = db_map.get_string(b"key3").unwrap();
        assert_eq!(r, None);
        //
        // large data
        let key = &"key9".repeat(25);
        let val = &"value8".repeat(70);
        let val2 = &"value9".repeat(70);
        // insert
        db_map.put_string(key, val).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(key, val2).unwrap();
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
        let r = db_map.get_string(b"").unwrap();
        assert_eq!(r, None);
        // insert empty key
        db_map.put_string(b"", val2).unwrap();
        // get empty key
        let r = db_map.get_string(b"").unwrap();
        assert_eq!(r, Some(val2.to_string()));
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_bytes<T: DbXxx<DbBytes>>(db_map: &mut T) {
        let key = b"The Adventure of the Missing Three-Quarter";
        let val = "We were fairly accustomed to receive weird telegrams at Baker Street,\n     but I have a particular recollection of one which reached us on a\n     gloomy February morning some seven or eight years";
        let val2 = "We were fairly accustomed to receive weird telegrams at Baker Street,\n     but I have a particular recollection of one which reached us on a\n     gloomy February morning some seven or eight years ago and gave Mr.\n     Sherlock Holmes a puzzled quarter of an hour.";
        // put
        db_map.put_string(key, val).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // modify
        db_map.put_string(key, val2).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
    }
    #[test]
    fn test_file_map_bytes_more() {
        let db_name = "target/tmp/test2-b.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_bytes("some_bytes_2").unwrap();
        basic_test_map_bytes(&mut db_map);
        medium_test_map_bytes(&mut db_map);
    }
}
