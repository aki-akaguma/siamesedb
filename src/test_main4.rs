use siamesedb::filedb::CheckFileDbMap;
use siamesedb::filedb::FileDbMapString;
use siamesedb::DbXxx;

fn main() {
    test_fixtures_fruits();
}

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

fn test_fixtures_fruits() {
    macro_rules! base_path {
        () => {
            ""
        }; //("/home/hcc/src/rust/MyJam/rel-github/lib/siamesedb/")
    }
    let db_name = concat!(base_path!(), "target/tmp/testAA.siamesedb");
    let _ = std::fs::remove_dir_all(db_name);
    let data = load_fixtures(concat!(base_path!(), "fixtures/test-fruits.txt"));
    let data = &data[..5000];
    //
    do_file_map_string(db_name, |mut db_map: FileDbMapString| {
        for (k, v) in data {
            db_map.put(k.clone(), v.as_bytes()).unwrap();
        }
        //
        db_map.sync_data().unwrap();
    });
    //
    do_file_map_string(db_name, |db_map: FileDbMapString| {
        assert!(db_map.is_balanced().unwrap());
        assert!(db_map.is_mst_valid().unwrap());
        assert!(db_map.is_dense().unwrap());
    });
    //
    do_file_map_string(db_name, |mut db_map: FileDbMapString| {
        for (k, v) in data {
            db_map.put(k.clone(), v.as_bytes()).unwrap();
        }
        //
        db_map.sync_data().unwrap();
    });
    //
    do_file_map_string(db_name, |db_map: FileDbMapString| {
        assert!(db_map.is_balanced().unwrap());
        assert!(db_map.is_mst_valid().unwrap());
        assert!(db_map.is_dense().unwrap());
    });
    //
    do_file_map_string(db_name, |mut db_map: FileDbMapString| {
        db_map
            .put_string("9909909900".to_string(), "TEST, v9909909900")
            .unwrap();
        db_map
            .put_string("9909909901".to_string(), "TEST, v9909909901")
            .unwrap();
        db_map
            .put_string("9909909902".to_string(), "TEST, v9909909902")
            .unwrap();
        db_map.sync_data().unwrap();
    });
    //
    do_file_map_string(db_name, |db_map: FileDbMapString| {
        assert!(db_map.is_balanced().unwrap());
        assert!(db_map.is_mst_valid().unwrap());
        assert!(db_map.is_dense().unwrap());
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
    /*
    do_file_map_string(db_name, |db_map: FileDbMapString| {
        println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    });
    return;
    */
    //
    do_file_map_string(db_name, |db_map: FileDbMapString| {
        assert!(db_map.is_balanced().unwrap());
        assert!(db_map.is_mst_valid().unwrap());
        assert!(db_map.is_dense().unwrap());
    });
    //
    do_file_map_string(db_name, |mut db_map: FileDbMapString| {
        assert_eq!(db_map.get_string("9909909900").unwrap(), None);
        assert_eq!(db_map.get_string("9909909901").unwrap(), None);
        assert_eq!(db_map.get_string("9909909902").unwrap(), None);
        //
        db_map.sync_data().unwrap();
        _print_check_db_map(
            &db_map,
            CheckC {
                _check: true,
                ..Default::default()
            },
        );
    });
    //
    do_file_map_string(db_name, |mut db_map: FileDbMapString| {
        db_map
            .put_string("9909909900".to_string(), "TEST, v9909909900")
            .unwrap();
        db_map
            .put_string("9909909901".to_string(), "TEST, v9909909901")
            .unwrap();
        db_map
            .put_string("9909909902".to_string(), "TEST, v9909909902")
            .unwrap();
        db_map.sync_data().unwrap();
        _print_check_db_map(
            &db_map,
            CheckC {
                _check: true,
                ..Default::default()
            },
        );
    });
    //
    do_file_map_string(db_name, |db_map: FileDbMapString| {
        assert!(db_map.is_balanced().unwrap());
        assert!(db_map.is_mst_valid().unwrap());
        assert!(db_map.is_dense().unwrap());
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

#[derive(Debug, Default, Clone, Copy)]
struct CheckC {
    _check: bool,
    _f_depth: bool,
    f_mst: bool,
    f_graph: bool,
}

fn _print_check_db_map(db_map: &FileDbMapString, check_cnf: CheckC) {
    if check_cnf.f_graph {
        println!("{}", db_map.graph_string_with_key_string().unwrap());
    }
    println!("record free: {:?}", db_map.count_of_free_record().unwrap());
    let (rec_v, node_v) = db_map.count_of_used_node().unwrap();
    println!("record used: {:?}", rec_v);
    println!("node free: {:?}", db_map.count_of_free_node().unwrap());
    println!("node used: {:?}", node_v);
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    if check_cnf.f_mst {
        println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    }
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    #[cfg(feature = "buf_stats")]
    println!("db_map.buf_stats(): {:?}", db_map.buf_stats());
    println!(
        "record_size_stats(): {}",
        db_map.record_size_stats().unwrap()
    );
}
