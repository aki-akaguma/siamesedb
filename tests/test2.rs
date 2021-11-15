mod test2 {
    use siamesedb::filedb::FileDbMapString;
    use siamesedb::DbMapString;
    ////
    fn do_file_map_string<F>(db_name: &str, mut fun: F)
    where
        F: FnMut(FileDbMapString),
    {
        let db = siamesedb::open_file(db_name).unwrap();
        let db_map = db.db_map_string("some_map1").unwrap();
        fun(db_map);
    }
    fn load_fixtures(path: &str) -> Vec<(String, String)> {
        use std::io::{BufRead, BufReader};
        //
        let mut vec = Vec::new();
        //
        let file = std::fs::File::open(path).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut line = String::new();
        while let Ok(size) = buf_reader.read_line(&mut line) {
            if size == 0 {
                break;
            }
            if let Some((a, b)) = line[..(line.len() - 1)].split_once(' ') {
                vec.push((a.to_string(), b.to_string()));
            }
            line.clear();
        }
        vec
    }
    ////
    #[test]
    fn test_fixtures_procs() {
        let data = load_fixtures("fixtures/test-procs.txt");
        let db_name = "target/tmp/test21.siamesedb";
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            for (k, v) in &data {
                db_map.put(k.as_str(), v.as_bytes()).unwrap();
            }
            //
            db_map.sync_data().unwrap();
        });
    }
    ////
    #[test]
    fn test_fixtures_fruits() {
        let data = load_fixtures("fixtures/test-fruits.txt");
        let data = &data[..100];
        let db_name = "target/tmp/test22.siamesedb";
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            for (k, v) in data {
                db_map.put(k.as_str(), v.as_bytes()).unwrap();
            }
            //
            db_map.sync_data().unwrap();
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            for (k, v) in data {
                db_map.put(k.as_str(), v.as_bytes()).unwrap();
            }
            //
            db_map.sync_data().unwrap();
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            db_map
                .put_string("9909909900", "TEST, v9909909900")
                .unwrap();
            db_map
                .put_string("9909909901", "TEST, v9909909901")
                .unwrap();
            db_map
                .put_string("9909909902", "TEST, v9909909902")
                .unwrap();
            db_map.sync_data().unwrap();
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            assert_eq!(
                db_map.get_string("9909909900").unwrap(),
                Some("TEST, v9909909900".to_string())
            );
            assert_eq!(
                db_map.get_string("9909909901").unwrap(),
                Some("TEST, v9909909901".to_string())
            );
            assert_eq!(
                db_map.get_string("9909909902").unwrap(),
                Some("TEST, v9909909902".to_string())
            );
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            db_map.delete("9909909900").unwrap();
            db_map.delete("9909909901").unwrap();
            db_map.delete("9909909902").unwrap();
            db_map.sync_data().unwrap();
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            assert_eq!(
                db_map.get_string("9909909900").unwrap(),
                None
            );
            assert_eq!(
                db_map.get_string("9909909901").unwrap(),
                None
            );
            assert_eq!(
                db_map.get_string("9909909902").unwrap(),
                None
            );
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            db_map
                .put_string("9909909900", "TEST, v9909909900")
                .unwrap();
            db_map
                .put_string("9909909901", "TEST, v9909909901")
                .unwrap();
            db_map
                .put_string("9909909902", "TEST, v9909909902")
                .unwrap();
            db_map.sync_data().unwrap();
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapString| {
            assert_eq!(
                db_map.get_string("9909909900").unwrap(),
                Some("TEST, v9909909900".to_string())
            );
            assert_eq!(
                db_map.get_string("9909909901").unwrap(),
                Some("TEST, v9909909901".to_string())
            );
            assert_eq!(
                db_map.get_string("9909909902").unwrap(),
                Some("TEST, v9909909902".to_string())
            );
        });
    }
}