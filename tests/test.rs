mod test {
    use siamesedb::{DbList, DbMap};
    //
    fn basic_test_map(db_map: &mut dyn DbMap) {
        // get nothing
        let r = db_map.get_string("key1").unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string("key1", "value1").unwrap();
        db_map.put_string("key2", "value2").unwrap();
        db_map.put_string("key3", "value3").unwrap();
        db_map.put_string("key4", "value4").unwrap();
        db_map.put_string("key5", "value5").unwrap();
        // get hits
        let r = db_map.get_string("key1").unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_map.get_string("key3").unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_map.get_string("key5").unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_map.put_string("key3", "VALUEVALUE3").unwrap();
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
        db_map.sync_data().unwrap();
    }
    fn basic_test_list(db_list: &mut dyn DbList) {
        // get nothing
        let r = db_list.get_string(1023).unwrap();
        assert_eq!(r, None);
        // insert
        db_list.put_string(1021, "value1").unwrap();
        db_list.put_string(1022, "value2").unwrap();
        db_list.put_string(1023, "value3").unwrap();
        db_list.put_string(1024, "value4").unwrap();
        db_list.put_string(1025, "value5").unwrap();
        // get hits
        let r = db_list.get_string(1021).unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_list.get_string(1023).unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_list.get_string(1025).unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_list.put_string(1023, "VALUEVALUE3").unwrap();
        let r = db_list.get_string(1023).unwrap();
        assert_eq!(r, Some("VALUEVALUE3".to_string()));
        // delete
        db_list.delete(1023).unwrap();
        let r = db_list.get_string(1023).unwrap();
        assert_eq!(r, None);
        //
        db_list.sync_data().unwrap();
    }
    fn medium_test_map(db_map: &mut dyn DbMap) {
        let key = "The Adventure of the Missing Three-Quarter";
        let val = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years";
        let val2 = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years ago and gave Mr.
     Sherlock Holmes a puzzled quarter of an hour.";
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
    fn medium_test_list(db_list: &mut dyn DbList) {
        let key = 123456789;
        let val = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years";
        let val2 = "We were fairly accustomed to receive weird telegrams at Baker Street,
     but I have a particular recollection of one which reached us on a
     gloomy February morning some seven or eight years ago and gave Mr.
     Sherlock Holmes a puzzled quarter of an hour.";
        db_list.put_string(key, val).unwrap();
        // get hits
        let r = db_list.get_string(key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // modify
        db_list.put_string(key, val2).unwrap();
        let r = db_list.get_string(key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_list.delete(key).unwrap();
        let r = db_list.get_string(key).unwrap();
        assert_eq!(r, None);
    }
    ////
    #[test]
    fn test_memory_map() {
        let db = siamesedb::open_memory();
        let mut db_map = db.db_map("some_map1");
        basic_test_map(&mut db_map);
    }
    #[test]
    fn test_memory_list() {
        let db = siamesedb::open_memory();
        let mut db_list = db.db_list("some_list1");
        basic_test_list(&mut db_list);
    }
    ////
    #[test]
    fn test_file_map() {
        let db_name = "target/tmp/test1.shamdb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map("some_map1").unwrap();
        basic_test_map(&mut db_map);
        medium_test_map(&mut db_map);
    }
    #[test]
    fn test_file_list() {
        let db_name = "target/tmp/test2.shamdb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_list = db.db_list("some_list1").unwrap();
        basic_test_list(&mut db_list);
        medium_test_list(&mut db_list);
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
