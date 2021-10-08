use shamdb::DbMap;

fn main() {
    //_test00();
    //_test01();
    //_test02();

    //_test03_10k();
    //_test04_10k();

    // 80.52user 62.24system 2:23.74elapsed 99%CPU (0avgtext+0avgdata 2156maxresident)k
    // 43.60user 3.91system 0:49.94elapsed 95%CPU (0avgtext+0avgdata 2364maxresident)k
    _test10_1m();
    //_test11_1m();
}

fn _test00() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    {
        db_map.put_string("key1", "value1").unwrap();
        db_map.put_string("key2", "value2").unwrap();
        db_map.put_string("key3", "value3").unwrap();
        db_map.put_string("key4", "value4").unwrap();
        db_map.put_string("key5", "value5").unwrap();
        //
        db_map.put_string("key6", "value6").unwrap();
        db_map.put_string("key7", "value7").unwrap();
        db_map.put_string("key8", "value8").unwrap();
        db_map.put_string("key9", "value9").unwrap();
        /*
         */
        /*
        db_map.put_string("key0", "value0").unwrap();
        db_map.put_string("key1", "value2").unwrap();
        db_map.put_string("key1", "value99").unwrap();
        */
        //
        db_map.sync_data().unwrap();
    }
    //
    //println!("{}", db_map.to_graph_string().unwrap());
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    println!();
    //
    {
        //db_map.delete("key1").unwrap();
        //db_map.delete("key2").unwrap();
        //db_map.delete("key3").unwrap();
        db_map.delete("key4").unwrap();
        //db_map.delete("key5").unwrap();
        //db_map.delete("key6").unwrap();
        //db_map.delete("key7").unwrap();
        //db_map.delete("key8").unwrap();
        //db_map.delete("key9").unwrap();
        //
        db_map.sync_data().unwrap();
    }
    //
    //println!("{}", db_map.to_graph_string().unwrap());
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
}

fn _test01() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, None);
    //
    db_map.put_string("key1", "value1").unwrap();
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value1".to_string()));
    //
    db_map.put_string("key2", "value2").unwrap();
    let r = db_map.get_string("key2").unwrap();
    assert_eq!(r, Some("value2".to_string()));
    //
    db_map.put_string("key0", "value0").unwrap();
    let r = db_map.get_string("key0").unwrap();
    assert_eq!(r, Some("value0".to_string()));
    //
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    db_map.put_string("key1", "value2").unwrap();
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value2".to_string()));
    //
    db_map.put_string("key1", "value99").unwrap();
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value99".to_string()));
    //
    db_map.sync_data().unwrap();
    //
    //println!("{}", db_map.to_graph_string().unwrap());
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
}

fn _test02() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    for i in 1..20 {
        let key = format!("key{:02}", i);
        let val = format!("value{:02}", i);
        db_map.put_string(&key, &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    //println!("{}", db_map.to_graph_string().unwrap());
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    println!();
    //
    let r = db_map.get_string("key01").unwrap();
    assert_eq!(r, Some("value01".to_string()));
    let r = db_map.get_string("key11").unwrap();
    assert_eq!(r, Some("value11".to_string()));
    let r = db_map.get_string("key19").unwrap();
    assert_eq!(r, Some("value19".to_string()));
    //
    db_map.delete("key11").unwrap();
    db_map.sync_data().unwrap();
    //
    //println!("{}", db_map.to_graph_string().unwrap());
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    //
    let r = db_map.get_string("key11").unwrap();
    assert_eq!(r, None);
}

fn _test03_10k() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    for i in 0..10_000 {
        let key = format!("key{:01}", i);
        let val = format!("value{:04}", i);
        db_map.put_string(&key, &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    println!("fin data");
    //
    //println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    //
    //let r = db_map.get_string("key0001");
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value0001".to_string()));
    let r = db_map.get_string("key2221").unwrap();
    assert_eq!(r, Some("value2221".to_string()));
    let r = db_map.get_string("key9991").unwrap();
    assert_eq!(r, Some("value9991".to_string()));
}

fn _test04_10k() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    for i in 0..10_000 {
        let key = format!("key{:04}", i);
        let val = format!("value{:04}", i);
        db_map.put_string(&key, &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    println!("fin data");
    //
    /*
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    */
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    //
    let r = db_map.get_string("key0001").unwrap();
    assert_eq!(r, Some("value0001".to_string()));
    let r = db_map.get_string("key2221").unwrap();
    assert_eq!(r, Some("value2221".to_string()));
    let r = db_map.get_string("key9991").unwrap();
    assert_eq!(r, Some("value9991".to_string()));
    //
    println!();
    //
    //db_map.delete("key2221");
    for i in 0..10000 {
        let key = format!("key{:04}", i);
        db_map.delete(&key).unwrap();
    }
    db_map.sync_data().unwrap();
    //
    let r = db_map.get_string("key2221").unwrap();
    assert_eq!(r, None);
    //
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
}

fn _test10_1m() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    for i in 0..1_000_000 {
        let key = format!("key{:01}", i);
        let val = format!("value{:01}", i);
        db_map.put_string(&key, &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    println!("fin data");
    //
    /*
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    */
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    //
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value1".to_string()));
    let r = db_map.get_string("key2221").unwrap();
    assert_eq!(r, Some("value2221".to_string()));
    let r = db_map.get_string("key9991").unwrap();
    assert_eq!(r, Some("value9991".to_string()));
    let r = db_map.get_string("key99999").unwrap();
    assert_eq!(r, Some("value99999".to_string()));
}

fn _test11_1m() {
    let db_name = "target/tmp/testA.shamdb";
    //let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let db_map = db.db_map("some_map1").unwrap();
    //
    /*
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    */
    /*
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    */
    //
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value1".to_string()));
    let r = db_map.get_string("key2221").unwrap();
    assert_eq!(r, Some("value2221".to_string()));
    let r = db_map.get_string("key9991").unwrap();
    assert_eq!(r, Some("value9991".to_string()));
    let r = db_map.get_string("key99999").unwrap();
    assert_eq!(r, Some("value99999".to_string()));
}
