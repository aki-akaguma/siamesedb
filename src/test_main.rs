use shamdb::DbMap;

fn main() {
    //_test00();
    //_test01();
    //_test02();

    //_test03_10k();
    //_test04_10k();

    // 80.52user 62.24system 2:23.74elapsed 99%CPU (0avgtext+0avgdata 2156maxresident)k
    // 43.60user 3.91system 0:49.94elapsed 95%CPU (0avgtext+0avgdata 2364maxresident)k
    // 32.91user 0.79system 0:33.95elapsed 99%CPU (0avgtext+0avgdata 2444maxresident)k
    //_test10_1m();
    //
    /*
        start put
        fin put
        free: [(31, 597), (71, 0), (103, 0), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        used: [(31, 38), (71, 138103), (103, 20978), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        db_map.is_balanced(): true
        db_map.is_dense(): true
        db_map.depth_of_node_tree(): 7
        start delete
        fin delete
        free: [(31, 635), (71, 138102), (103, 20978), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        used: [(31, 0), (71, 1), (103, 0), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        db_map.is_balanced(): true
        db_map.is_dense(): true
        db_map.depth_of_node_tree(): 1
        56.85user 1.19system 0:58.93elapsed 98%CPU (0avgtext+0avgdata 2196maxresident)k
        8inputs+115480outputs (0major+122minor)pagefaults 0swaps
    */
    //
    _test11_1m();
    //
    //
    //
    /*
        $ /usr/bin/time target/release/test_main
        start put
        fin put
        free: [(31, 1), (71, 1), (103, 0), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        used: [(31, 599), (71, 13410), (103, 1901), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        db_map.depth_of_node_tree(): 6
        2.27user 0.13system 0:02.45elapsed 98%CPU (0avgtext+0avgdata 2384maxresident)k
        0inputs+6184outputs (0major+129minor)pagefaults 0swaps
        $
        $ ll target/tmp/testA.shamdb/
        -rw-rw-r-- 1 hcc hcc 1.9M 10月 14 12:26 some_map1.dat
        -rw-rw-r-- 1 hcc hcc 1.2M 10月 14 12:26 some_map1.idx
        -rw-rw-r-- 1 hcc hcc   64 10月 14 12:26 some_map1.unu
    */
    // 100k
    //_test_put_only(100_000);
    //
    /*
        $ /usr/bin/time target/release/test_main
        start put
        fin put
        free: [(31, 597), (71, 0), (103, 0), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        used: [(31, 38), (71, 138103), (103, 20978), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        db_map.depth_of_node_tree(): 7
        28.68user 1.05system 0:30.12elapsed 98%CPU (0avgtext+0avgdata 2312maxresident)k
        32inputs+66272outputs (0major+142minor)pagefaults 0swaps
        $
        $ ll target/tmp/testA.shamdb/
        -rw-rw-r-- 1 hcc hcc 21M 10月 14 12:16 some_map1.dat
        -rw-rw-r-- 1 hcc hcc 12M 10月 14 12:16 some_map1.idx
        -rw-rw-r-- 1 hcc hcc  64 10月 14 12:16 some_map1.unu
    */
    // 1m
    //_test_put_only(1_000_000);
    //
    /*
       $ /usr/bin/time target/release/test_main
       start put
       fin put
       free: [(31, 635), (71, 0), (103, 0), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
       used: [(31, 0), (71, 1380794), (103, 210487), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
       db_map.depth_of_node_tree(): 8
       323.41user 4.21system 5:29.92elapsed 99%CPU (0avgtext+0avgdata 2504maxresident)k
       0inputs+727152outputs (0major+159minor)pagefaults 0swaps
       $
       $ ll target/tmp/testA.shamdb/
       -rw-rw-r-- 1 hcc hcc 227M 10月 14 12:24 some_map1.dat
       -rw-rw-r-- 1 hcc hcc 116M 10月 14 12:24 some_map1.idx
       -rw-rw-r-- 1 hcc hcc   64 10月 14 12:24 some_map1.unu
    */
    //_test_put_only(10_000_000); // 10m
    //
    //
    /*
        $ /usr/bin/time target/release/test_main
        start put
        fin put
        free: [(31, 635), (71, 0), (103, 0), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
        used: [(31, 0), (71, 10230192), (103, 3713564), (143, 1968956), (175, 0), (215, 0), (231, 0), (256, 0)]
        db_map.depth_of_node_tree(): 8
        3942.16user 72.20system 1:08:32elapsed 97%CPU (0avgtext+0avgdata 2272maxresident)k
        1860600inputs+8070832outputs (0major+161minor)pagefaults 0swaps
        $
        $ ll target/tmp/testA.shamdb/
        -rw-rw-r-- 1 hcc hcc 2.5G 10月 14 12:09 some_map1.dat
        -rw-rw-r-- 1 hcc hcc 1.4G 10月 14 12:09 some_map1.idx
        -rw-rw-r-- 1 hcc hcc   64 10月 14 12:09 some_map1.unu
    */
    // 100m
    //_test_put_only(100_000_000);
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
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
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
    db_map.delete("key19").unwrap();
    db_map.delete("key18").unwrap();
    db_map.delete("key17").unwrap();
    db_map.delete("key16").unwrap();
    /*
     */
    db_map.delete("key15").unwrap();
    db_map.delete("key14").unwrap();
    db_map.delete("key13").unwrap();
    db_map.delete("key12").unwrap();
    db_map.delete("key11").unwrap();
    db_map.delete("key10").unwrap();
    //
    /*
     */
    db_map.delete("key09").unwrap();
    db_map.delete("key08").unwrap();
    db_map.delete("key07").unwrap();
    db_map.delete("key06").unwrap();
    db_map.delete("key05").unwrap();
    db_map.delete("key04").unwrap();
    db_map.delete("key03").unwrap();
    db_map.delete("key02").unwrap();
    db_map.delete("key01").unwrap();
    //
    db_map.sync_data().unwrap();
    //
    //println!("{}", db_map.to_graph_string().unwrap());
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    //
    let r = db_map.get_string("key19").unwrap();
    assert_eq!(r, None);
}

fn _test03_10k() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    println!("start put");
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
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    //
    eprintln!("key1");
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value0001".to_string()));
    eprintln!("key2221");
    let r = db_map.get_string("key2221").unwrap();
    assert_eq!(r, Some("value2221".to_string()));
    eprintln!("key9991");
    let r = db_map.get_string("key9991").unwrap();
    assert_eq!(r, Some("value9991".to_string()));
}

fn _test04_10k() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    println!("start put");
    //
    for i in 0..10_000 {
        let key = format!("key{:04}", i);
        let val = format!("value{:04}", i);
        db_map.put_string(&key, &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    println!("fin put");
    //
    //println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
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
    for i in 0..10_000 {
        let key = format!("key{:04}", i);
        db_map.delete(&key).unwrap();
    }
    db_map.sync_data().unwrap();
    //
    println!("fin delete");
    //
    //let r = db_map.get_string("key2221").unwrap();
    //assert_eq!(r, None);
    //
    //println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
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
    println!("start put");
    //
    for i in 0..1_000_000 {
        let key = format!("key{:01}", i);
        let val = format!("value{:01}", i);
        db_map.put_string(&key, &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    println!("fin put");
    //
    /*
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    */
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
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
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    const MAX_CNT: u64 = 1_000_000;
    //
    println!("start put");
    //
    for i in 0..MAX_CNT {
        let key = format!("key{:01}", i);
        let val = format!("value{:01}", i);
        db_map.put_string(&key, &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    println!("fin put");
    //
    //println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    //println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    //
    println!("start delete");
    //
    for i in 0..MAX_CNT {
        let key = format!("key{:01}", i);
        db_map.delete(&key).unwrap();
    }
    db_map.sync_data().unwrap();
    //
    println!("fin delete");
    //
    //let r = db_map.get_string("key2221").unwrap();
    //assert_eq!(r, None);
    //
    //println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    //println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
}

fn _test_put_only(max_cnt: u64) {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = shamdb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    println!("start put");
    //
    for i in 0..max_cnt {
        let key = format!("key{:01}", i);
        let val = format!("value{:01}", i);
        db_map.put_string(&key, &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    println!("fin put");
    //
    /*
    println!("{}", db_map.to_graph_string_with_key_string().unwrap());
    */
    println!("free: {:?}", db_map.count_of_free_node().unwrap());
    println!("used: {:?}", db_map.count_of_used_node().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    /*
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    //println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    */
    //
    /*
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value1".to_string()));
    let r = db_map.get_string("key2221").unwrap();
    assert_eq!(r, Some("value2221".to_string()));
    let r = db_map.get_string("key9991").unwrap();
    assert_eq!(r, Some("value9991".to_string()));
    let r = db_map.get_string("key99999").unwrap();
    assert_eq!(r, Some("value99999".to_string()));
    */
}
