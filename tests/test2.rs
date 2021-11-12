mod test2 {
    use siamesedb::filedb::FileDbMapString;
    use siamesedb::DbMapString;
    ////
    fn do_file_map_string<F>(mut fun: F)
    where
        F: FnMut(FileDbMapString) -> (),
    {
        let db_name = "target/tmp/test21.siamesedb";
        let db = siamesedb::open_file(db_name).unwrap();
        let db_map = db.db_map_string("some_map1").unwrap();
        fun(db_map);
    }
    fn load_fixtures_procs() -> Vec<(String, String)> {
        use std::io::{BufRead, BufReader};
        //
        let mut vec = Vec::new();
        //
        let file = std::fs::File::open("fixtures/test-procs.txt").unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut line = String::new();
        while let Ok(size) = buf_reader.read_line(&mut line) {
            if size == 0 {
                break;
            }
            if let Some((a, b)) = line.split_once(' ') {
                vec.push((a.to_string(), b.to_string()));
            }
        }
        vec
    }
    ////
    #[test]
    fn test_fixtures_procs() {
        let data = load_fixtures_procs();
        //
        do_file_map_string(|mut db_map: FileDbMapString| {
            for (k, v) in &data {
                db_map.put(k.as_str(), v.as_bytes()).unwrap();
            }
            //
            db_map.sync_data().unwrap();
        });
    }
}
