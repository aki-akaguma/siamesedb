mod test {
    use siamesedb::{DbInt, DbXxx};
    //
    fn basic_test_map_dbint<T: DbXxx<DbInt>>(db_map: &mut T) {
        // get nothing
        let r = db_map.get_string(&1023).unwrap();
        assert_eq!(r, None);
        // insert
        db_map.put_string(&1021, "value1").unwrap();
        db_map.put_string(&1022, "value2").unwrap();
        db_map.put_string(&1023, "value3").unwrap();
        db_map.put_string(&1024, "value4").unwrap();
        db_map.put_string(&1025, "value5").unwrap();
        // get hits
        let r = db_map.get_string(&1021).unwrap();
        assert_eq!(r, Some("value1".to_string()));
        let r = db_map.get_string(&1023).unwrap();
        assert_eq!(r, Some("value3".to_string()));
        let r = db_map.get_string(&1025).unwrap();
        assert_eq!(r, Some("value5".to_string()));
        // modify
        db_map.put_string(&1023, "VALUEVALUE3").unwrap();
        let r = db_map.get_string(&1023).unwrap();
        assert_eq!(r, Some("VALUEVALUE3".to_string()));
        // delete
        db_map.delete(&1023).unwrap();
        let r = db_map.get_string(&1023).unwrap();
        assert_eq!(r, None);
        //
        db_map.sync_data().unwrap();
    }
    fn medium_test_map_dbint<T: DbXxx<DbInt>>(db_map: &mut T) {
        let key = 123456789;
        let val = "We were fairly accustomed to receive weird telegrams at Baker Street,\n     but I have a particular recollection of one which reached us on a\n     gloomy February morning some seven or eight years";
        let val2 = "We were fairly accustomed to receive weird telegrams at Baker Street,\n     but I have a particular recollection of one which reached us on a\n     gloomy February morning some seven or eight years ago and gave Mr.\n     Sherlock Holmes a puzzled quarter of an hour.";
        db_map.put_string(&key, val).unwrap();
        // get hits
        let r = db_map.get_string(&key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // modify
        db_map.put_string(&key, val2).unwrap();
        let r = db_map.get_string(&key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(&key).unwrap();
        let r = db_map.get_string(&key).unwrap();
        assert_eq!(r, None);
    }
    #[test]
    fn test_memory_map_dbint_more() {
        let db = siamesedb::open_memory();
        let mut db_map = db.db_map_dbint("some_u64_2");
        basic_test_map_dbint(&mut db_map);
        medium_test_map_dbint(&mut db_map);
    }
}
