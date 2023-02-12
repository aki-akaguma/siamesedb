mod test2 {
    use siamesedb::filedb::CheckFileDbMap;
    use siamesedb::filedb::FileDbMapDbString;
    use siamesedb::{DbXxx, DbXxxBase};
    ////
    fn do_file_map_string<F>(db_name: &str, mut fun: F)
    where
        F: FnMut(FileDbMapDbString),
    {
        let db = siamesedb::open_file(db_name).unwrap();
        let db_map = db.db_map_string("some_map1").unwrap();
        fun(db_map);
    }
    fn load_fixtures(count: Option<usize>, path: &str) -> Vec<(String, String)> {
        use std::io::{BufRead, BufReader};
        //
        let mut vec = Vec::new();
        //
        let file = std::fs::File::open(path).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut line = String::new();
        #[rustfmt::skip]
        let mut counter = if count.is_some() { count.unwrap() } else { usize::MAX };
        while let Ok(size) = buf_reader.read_line(&mut line) {
            if size == 0 || counter == 0 {
                break;
            }
            if let Some((a, b)) = line[..(line.len() - 1)].split_once(' ') {
                vec.push((a.to_string(), b.to_string()));
            }
            line.clear();
            counter -= 1;
        }
        vec
    }
    ////
    #[test]
    fn test_fixtures_procs() {
        #[rustfmt::skip]
        let count = if cfg!(miri) { Some(10) } else { None };
        let data = load_fixtures(count, "fixtures/test-procs.txt");
        let db_name = "target/tmp/test21.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
            for (k, v) in &data {
                db_map.put(k, v.as_bytes()).unwrap();
            }
            //
            db_map.sync_data().unwrap();
        });
    }
    ////
    #[test]
    fn test_fixtures_fruits() {
        #[rustfmt::skip]
        let count = if cfg!(miri) { Some(10) } else { None };
        let data = load_fixtures(count, "fixtures/test-fruits.txt");
        let db_name = "target/tmp/test22.siamesedb";
        let _ = std::fs::remove_dir_all(db_name);
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
            for (k, v) in &data {
                db_map.put(k, v.as_bytes()).unwrap();
            }
            //
            db_map.flush().unwrap();
        });
        //
        do_file_map_string(db_name, |db_map: FileDbMapDbString| {
            assert!(db_map.is_balanced().unwrap());
            assert!(db_map.is_mst_valid().unwrap());
            assert!(db_map.is_dense().unwrap());
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
            for (k, v) in &data {
                db_map.put(k, v.as_bytes()).unwrap();
            }
            //
            db_map.flush().unwrap();
        });
        //
        do_file_map_string(db_name, |db_map: FileDbMapDbString| {
            assert!(db_map.is_balanced().unwrap());
            assert!(db_map.is_mst_valid().unwrap());
            assert!(db_map.is_dense().unwrap());
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
            db_map
                .put_string("9909909900", "TEST, v9909909900")
                .unwrap();
            db_map
                .put_string("9909909901", "TEST, v9909909901")
                .unwrap();
            db_map
                .put_string("9909909902", "TEST, v9909909902")
                .unwrap();
            db_map.flush().unwrap();
        });
        //
        do_file_map_string(db_name, |db_map: FileDbMapDbString| {
            assert!(db_map.is_balanced().unwrap());
            assert!(db_map.is_mst_valid().unwrap());
            assert!(db_map.is_dense().unwrap());
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
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
        do_file_map_string(db_name, |db_map: FileDbMapDbString| {
            assert!(db_map.is_balanced().unwrap());
            assert!(db_map.is_mst_valid().unwrap());
            assert!(db_map.is_dense().unwrap());
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
            db_map.delete("9909909900").unwrap();
            db_map.delete("9909909901").unwrap();
            db_map.delete("9909909902").unwrap();
            db_map.flush().unwrap();
        });
        //
        do_file_map_string(db_name, |db_map: FileDbMapDbString| {
            assert!(db_map.is_balanced().unwrap());
            assert!(db_map.is_mst_valid().unwrap());
            assert!(db_map.is_dense().unwrap());
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
            assert_eq!(db_map.get_string("9909909900").unwrap(), None);
            assert_eq!(db_map.get_string("9909909901").unwrap(), None);
            assert_eq!(db_map.get_string("9909909902").unwrap(), None);
        });
        //
        do_file_map_string(db_name, |db_map: FileDbMapDbString| {
            assert!(db_map.is_balanced().unwrap());
            assert!(db_map.is_mst_valid().unwrap());
            assert!(db_map.is_dense().unwrap());
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
            db_map
                .put_string("9909909900", "TEST, v9909909900")
                .unwrap();
            db_map
                .put_string("9909909901", "TEST, v9909909901")
                .unwrap();
            db_map
                .put_string("9909909902", "TEST, v9909909902")
                .unwrap();
            db_map.flush().unwrap();
        });
        //
        do_file_map_string(db_name, |db_map: FileDbMapDbString| {
            assert!(db_map.is_balanced().unwrap());
            assert!(db_map.is_mst_valid().unwrap());
            assert!(db_map.is_dense().unwrap());
        });
        //
        do_file_map_string(db_name, |mut db_map: FileDbMapDbString| {
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
        do_file_map_string(db_name, |db_map: FileDbMapDbString| {
            assert!(db_map.is_balanced().unwrap());
            assert!(db_map.is_mst_valid().unwrap());
            assert!(db_map.is_dense().unwrap());
        });
    }
}
