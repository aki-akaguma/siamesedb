mod test {
    use shamdb::{DbList, DbMap};
    //
    fn basic_test_map(db_map: &mut dyn DbMap) {
        let r = db_map.get_string("key1").unwrap();
        assert_eq!(r, None);
        db_map.put_string("key1", "value1").unwrap();
        let r = db_map.get_string("key1").unwrap();
        assert_eq!(r, Some("value1".to_string()));
        db_map.sync_data().unwrap();
    }
    fn basic_test_list(db_list: &mut dyn DbList) {
        let r = db_list.get_string(1023).unwrap();
        assert_eq!(r, None);
        db_list.put_string(1023, "value1").unwrap();
        let r = db_list.get_string(1023).unwrap();
        assert_eq!(r, Some("value1".to_string()));
        db_list.sync_data().unwrap();
    }
    ////
    #[test]
    fn test_memory_map() {
        let db = shamdb::open_memory();
        let mut db_map = db.db_map("some_map1");
        basic_test_map(&mut db_map);
    }
    #[test]
    fn test_memory_list() {
        let db = shamdb::open_memory();
        let mut db_list = db.db_list("some_list1");
        basic_test_list(&mut db_list);
    }
    ////
    #[test]
    fn test_file_map() {
        let db_name = "target/tmp/test1.shamdb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = shamdb::open_file(db_name).unwrap();
        let mut db_map = db.db_map("some_map1").unwrap();
        basic_test_map(&mut db_map);
    }
    #[test]
    fn test_file_list() {
        let db_name = "target/tmp/test2.shamdb";
        let _ = std::fs::remove_dir_all(db_name);
        let db = shamdb::open_file(db_name).unwrap();
        let mut db_list = db.db_list("some_list1").unwrap();
        basic_test_list(&mut db_list);
    }
}
