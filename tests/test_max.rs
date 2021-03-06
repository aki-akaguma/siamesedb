#[cfg(feature = "test_large_data")]
mod test {
    use siamesedb::{DbString, DbXxx};
    ////
    #[test]
    fn test_file_map() {
        let db_name = "target/tmp/test3.shamdb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = siamesedb::open_file(db_name).unwrap();
        let mut db_map = db.db_map_string("some_map_max1").unwrap();
        maximum_test_map_string(&mut db_map);
    }
    fn maximum_test_map_string<T: DbXxx<DbString>>(db_map: &mut T) {
        let key = "The Adventure of the Missing Three-Quarter";
        let val = "We were fairly accustomed to receive weird telegrams at Baker Street, but I have a particular recollection of one which reached us on a gloomy February morning some seven or eight years";
        let val2 = "abc".repeat(256 * 1024 * 1024);
        assert!(val2.len() >= 768 * 1024 * 1024);
        // put
        db_map.put_string(key, val).unwrap();
        // get hits
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val.to_string()));
        // modify
        db_map.put_string(key, &val2).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, Some(val2.to_string()));
        // delete
        db_map.delete(key).unwrap();
        let r = db_map.get_string(key).unwrap();
        assert_eq!(r, None);
    }
}
