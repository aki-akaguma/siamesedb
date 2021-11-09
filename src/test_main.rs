use siamesedb::filedb::FileDbList;
use siamesedb::filedb::FileDbMap;
use siamesedb::DbList;
use siamesedb::DbMap;

fn main() {
    _test_a1();
    //_test_a2();
}
fn _test_a1() {
    //_test00_map();
    //_test00_list();
    //
    //_test01();
    //_test02();
    //
    //
    // 1m
    //
    /*
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 1_000, f_get: false, f_delete: false, f_repeat: 1 },
        CheckC { check: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 1_000, f_get: true, f_delete: true, f_repeat: 3 },
        CheckC { check: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i).repeat(25) },
        |i: usize| { format!("value{:01}", i).repeat(60) },
    );
    */
    /*
    #[rustfmt::skip]
    _test_db_list(
        TestC { max_cnt: 1_000, f_get: true, f_delete: true, f_repeat: 10 },
        CheckC { check: true, ..Default::default() },
        /*
        |i: usize| { i as u64 },
        |i: usize| { format!("value{:01}", i) },
        */
        /*
        */
        |i: usize| { i as u64 },
        |i: usize| { format!("value{:01}", i).repeat(70) },
    );
    */
    /*
    start put
    finish put: 24.892µs/op
    record free: [(15, 0), (23, 0), (31, 0), (47, 0), (63, 0), (255, 0), (511, 0), (2047, 0)]
    node free: [(31, 524), (71, 0), (103, 0), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
    record used: [(15, 100), (23, 999900), (31, 0), (47, 0), (63, 0), (255, 0), (511, 0), (2047, 0)]
    node used: [(31, 0), (71, 138135), (103, 20984), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
    db_map.is_balanced(): true
    db_map.is_dense(): true
    db_map.depth_of_node_tree(): 7
    start get
    finish get: 9.305µs/op
    start delete
    finish delete: 22.545µs/op
    record free: [(15, 100), (23, 999900), (31, 0), (47, 0), (63, 0), (255, 0), (511, 0), (2047, 0)]
    node free: [(31, 524), (71, 138134), (103, 20984), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
    record used: [(15, 0), (23, 0), (31, 0), (47, 0), (63, 0), (255, 0), (511, 0), (2047, 0)]
    node used: [(31, 0), (71, 1), (103, 0), (143, 0), (175, 0), (215, 0), (231, 0), (256, 0)]
    db_map.is_balanced(): true
    db_map.is_dense(): true
    db_map.depth_of_node_tree(): 1
    56.70user 0.97system 0:58.23elapsed 99%CPU (0avgtext+0avgdata 2304maxresident)k
    0inputs+141216outputs (0major+213minor)pagefaults 0swaps
    */
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 1_000_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
    /*
    #[rustfmt::skip]
    _test_db_list(
        TestC { max_cnt: 1_000_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: true, ..Default::default() },
        |i: usize| { i as u64 },
        |i: usize| { format!("value{:01}", i) },
    );
     */
}

fn _test_a2() {
    //
    // 10k
    //
    // 0.35user 0.01system 0:00.38elapsed 93%CPU (0avgtext+0avgdata 2208maxresident)k
    //
    // finish put: 14.096µs/op
    // db_map.depth_of_node_tree(): 5
    // finish get: 3.341µs/op
    // finish delete: 9.314µs/op
    // 0.26user 0.00system 0:00.27elapsed 96%CPU (0avgtext+0avgdata 2396maxresident)k
    /*
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 10_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: false, f_depth: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
    */
    //
    // 100k
    //
    // 4.57user 0.09system 0:04.78elapsed 97%CPU (0avgtext+0avgdata 2332maxresident)k
    //
    // finish put: 16.62µs/op
    // db_map.depth_of_node_tree(): 6
    // finish get: 4.021µs/op
    // finish delete: 11.396µs/op
    // 3.11user 0.07system 0:03.23elapsed 98%CPU (0avgtext+0avgdata 2272maxresident)k
    /*
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 100_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: false, f_depth: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
     */
    //
    // 1m
    //
    // 54.47user 0.99system 0:56.10elapsed 98%CPU (0avgtext+0avgdata 2264maxresident)k
    //
    // finish put: 19.782µs/op
    // db_map.depth_of_node_tree(): 7
    // finish get: 4.714µs/op
    // finish delete: 12.943µs/op
    // 36.39user 0.78system 0:37.60elapsed 98%CPU (0avgtext+0avgdata 2336maxresident)k
    /*
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 1_000_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: false, f_depth: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
     */
    //
    // 10m
    //
    // 627.91user 12.64system 10:51.87elapsed 98%CPU (0avgtext+0avgdata 2344maxresident)k
    //
    // finish put: 22.899µs/op
    // db_map.depth_of_node_tree(): 8
    // finish get: 4.981µs/op
    // finish delete: 15.317µs/op
    // 415.67user 11.13system 7:14.21elapsed 98%CPU (0avgtext+0avgdata 2444maxresident)k
    /*
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 10_000_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: false, f_depth: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
    */
    //
    //
    // 100m
    //
    // 7466.42user 229.81system 2:15:24elapsed 94%CPU (0avgtext+0avgdata 2224maxresident)k
    //
    // finish put: 27.208µs/op
    // db_map.depth_of_node_tree(): 9
    // finish get: 7.15µs/op
    // finish delete: 22.514µs/op
    // 4875.26user 231.29system 1:34:47elapsed 89%CPU (0avgtext+0avgdata 2448maxresident)k
    //
    // node cache size: 64
    //
    // record free: [(16, 0), (24, 0), (32, 0), (48, 0), (64, 0), (256, 0), (512, 0), (2048, 0)]
    // record used: [(16, 100), (24, 999900), (32, 99000000), (48, 0), (64, 0), (256, 0), (512, 0), (2048, 0)]
    // node free: [(32, 503), (72, 1), (104, 0), (144, 0), (176, 0), (216, 0), (232, 0), (256, 0)]
    // node used: [(32, 0), (72, 8605028), (104, 5307029), (144, 2000655), (176, 0), (216, 0), (232, 0), (256, 0)]
    // db_map.is_balanced(): true
    // db_map.is_dense(): true
    // record_size_stats(): [(16, 100), (24, 999900), (32, 99000000)]
    //
    // finish put: 27.041µs/op
    // db_map.depth_of_node_tree(): 9
    // finish get: 5.424µs/op
    // finish delete: 17.437µs/op
    // 5050.66user 153.20system 1:27:33elapsed 99%CPU (0avgtext+0avgdata 2556maxresident)k
    /*
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 100_000_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: false, f_depth: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
    */
    // 3194.15user 44.34system 54:27.41elapsed 99%CPU (0avgtext+0avgdata 2408maxresident)k
    //_test_map_put_only(100_000_000, false, false);
}

fn _test00_map() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
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

fn _test00_list() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_list = db.db_list("some_list1").unwrap();
    //
    {
        db_list.put_string(1, "value1").unwrap();
        db_list.put_string(2, "value2").unwrap();
        db_list.put_string(3, "value3").unwrap();
        db_list.put_string(4, "value4").unwrap();
        db_list.put_string(5, "value5").unwrap();
        //
        db_list.put_string(6, "value6").unwrap();
        db_list.put_string(7, "value7").unwrap();
        db_list.put_string(8, "value8").unwrap();
        db_list.put_string(9, "value9").unwrap();
        /*
         */
        /*
        db_map.put_string("key0", "value0").unwrap();
        db_map.put_string("key1", "value2").unwrap();
        db_map.put_string("key1", "value99").unwrap();
        */
        //
        db_list.sync_data().unwrap();
    }
    //
    //println!("{}", db_list.to_graph_string().unwrap());
    println!("{}", db_list.to_graph_string_with_key_string().unwrap());
    println!("db_list.is_balanced(): {}", db_list.is_balanced().unwrap());
    println!(
        "db_list.is_mst_valid(): {}",
        db_list.is_mst_valid().unwrap()
    );
    println!("db_list.is_dense(): {}", db_list.is_dense().unwrap());
    println!(
        "db_list.depth_of_node_tree(): {}",
        db_list.depth_of_node_tree().unwrap()
    );
    println!();
    //
    {
        //db_map.delete("key1").unwrap();
        //db_map.delete("key2").unwrap();
        //db_map.delete("key3").unwrap();
        db_list.delete(4).unwrap();
        //db_map.delete("key5").unwrap();
        //db_map.delete("key6").unwrap();
        //db_map.delete("key7").unwrap();
        //db_map.delete("key8").unwrap();
        //db_map.delete("key9").unwrap();
        //
        db_list.sync_data().unwrap();
    }
    //
    //println!("{}", db_list.to_graph_string().unwrap());
    println!("{}", db_list.to_graph_string_with_key_string().unwrap());
    println!("db_list.is_balanced(): {}", db_list.is_balanced().unwrap());
    println!(
        "db_list.is_mst_valid(): {}",
        db_list.is_mst_valid().unwrap()
    );
    println!("db_list.is_dense(): {}", db_list.is_dense().unwrap());
    println!(
        "db_list.depth_of_node_tree(): {}",
        db_list.depth_of_node_tree().unwrap()
    );
}

fn _test01() {
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
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
    let db = siamesedb::open_file(db_name).unwrap();
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
    println!("record free: {:?}", db_map.count_of_free_record().unwrap());
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
    println!("record free: {:?}", db_map.count_of_free_record().unwrap());
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

#[derive(Debug, Default, Clone)]
struct TestC {
    max_cnt: usize,
    f_get: bool,
    f_delete: bool,
    f_repeat: usize,
}

#[derive(Debug, Default, Clone, Copy)]
struct CheckC {
    check: bool,
    f_depth: bool,
    f_mst: bool,
    f_graph: bool,
}

use std::convert::TryInto;

fn _test_db_map<FK, FV>(test_cnf: TestC, check_cnf: CheckC, fmt_key_func: FK, fmt_val_func: FV)
where
    FK: Fn(usize) -> String,
    FV: Fn(usize) -> String,
{
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map("some_map1").unwrap();
    //
    for _ in 0..test_cnf.f_repeat.max(1) {
        println!("start put");
        let instant_st = std::time::Instant::now();
        for i in 0..test_cnf.max_cnt {
            let key = fmt_key_func(i);
            let val = fmt_val_func(i);
            db_map.put_string(&key, &val).unwrap();
        }
        let instant_ed = std::time::Instant::now();
        let instant_per_op = (instant_ed - instant_st) / test_cnf.max_cnt.try_into().unwrap();
        println!("finish put: {:?}/op", instant_per_op);
        //
        db_map.sync_data().unwrap();
        //
        if check_cnf.check {
            _print_check_db_map(&db_map, check_cnf);
        } else if check_cnf.f_depth {
            _print_depth_db_map(&db_map);
        }
        //
        if test_cnf.f_get {
            println!("start get");
            let instant_st = std::time::Instant::now();
            for i in 0..test_cnf.max_cnt {
                let key = fmt_key_func(i);
                let val = fmt_val_func(i);
                let s = db_map.get_string(&key).unwrap();
                assert!(Some(val) == s, "key: {}, s: {:?}", key, s);
            }
            let instant_ed = std::time::Instant::now();
            let instant_per_op = (instant_ed - instant_st) / test_cnf.max_cnt.try_into().unwrap();
            println!("finish get: {:?}/op", instant_per_op);
        }
        //
        if test_cnf.f_delete {
            println!("start delete");
            let instant_st = std::time::Instant::now();
            for i in 0..test_cnf.max_cnt {
                let key = fmt_key_func(i);
                db_map.delete(&key).unwrap();
            }
            let instant_ed = std::time::Instant::now();
            let instant_per_op = (instant_ed - instant_st) / test_cnf.max_cnt.try_into().unwrap();
            println!("finish delete: {:?}/op", instant_per_op);
            //
            db_map.sync_data().unwrap();
            //
            if check_cnf.check {
                _print_check_db_map(&db_map, check_cnf);
            }
        }
    }
}

fn _test_db_list<FK, FV>(test_cnf: TestC, check_cnf: CheckC, fmt_key_func: FK, fmt_val_func: FV)
where
    FK: Fn(usize) -> u64,
    FV: Fn(usize) -> String,
{
    let db_name = "target/tmp/testA.shamdb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_list = db.db_list("some_list1").unwrap();
    //
    for _ in 0..test_cnf.f_repeat.max(1) {
        println!("start put");
        let instant_st = std::time::Instant::now();
        for i in 0..test_cnf.max_cnt {
            /*
            let key = i as u64;
            let val = format!("value{:01}", i);
            */
            let key = fmt_key_func(i);
            let val = fmt_val_func(i);
            db_list.put_string(key, &val).unwrap();
        }
        let instant_ed = std::time::Instant::now();
        let instant_per_op = (instant_ed - instant_st) / test_cnf.max_cnt.try_into().unwrap();
        println!("finish put: {:?}/op", instant_per_op);
        //
        db_list.sync_data().unwrap();
        //
        if check_cnf.check {
            _print_check_db_list(&db_list, check_cnf);
        }
        //
        if test_cnf.f_get {
            println!("start get");
            let instant_st = std::time::Instant::now();
            for i in 0..test_cnf.max_cnt {
                let key = fmt_key_func(i);
                let val = fmt_val_func(i);
                let s = db_list.get_string(key).unwrap();
                assert!(Some(val) == s, "key: {}, s: {:?}", key, s);
            }
            let instant_ed = std::time::Instant::now();
            let instant_per_op = (instant_ed - instant_st) / test_cnf.max_cnt.try_into().unwrap();
            println!("finish get: {:?}/op", instant_per_op);
        }
        //
        if test_cnf.f_delete {
            println!("start delete");
            let instant_st = std::time::Instant::now();
            for i in 0..test_cnf.max_cnt {
                let key = fmt_key_func(i);
                db_list.delete(key).unwrap();
            }
            let instant_ed = std::time::Instant::now();
            let instant_per_op = (instant_ed - instant_st) / test_cnf.max_cnt.try_into().unwrap();
            println!("finish delete: {:?}/op", instant_per_op);
            //
            db_list.sync_data().unwrap();
            //
            if check_cnf.check {
                _print_check_db_list(&db_list, check_cnf);
            }
        }
    }
}

fn _print_check_db_map(db_map: &FileDbMap, check_cnf: CheckC) {
    if check_cnf.f_graph {
        println!("{}", db_map.to_graph_string_with_key_string().unwrap());
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

fn _print_check_db_list(db_list: &FileDbList, check_cnf: CheckC) {
    if check_cnf.f_graph {
        println!("{}", db_list.to_graph_string_with_key_string().unwrap());
    }
    println!("record free: {:?}", db_list.count_of_free_record().unwrap());
    let (rec_v, node_v) = db_list.count_of_used_node().unwrap();
    println!("record used: {:?}", rec_v);
    println!("node free: {:?}", db_list.count_of_free_node().unwrap());
    println!("node used: {:?}", node_v);
    println!("db_list.is_balanced(): {}", db_list.is_balanced().unwrap());
    if check_cnf.f_mst {
        println!("db_map.is_mst_valid(): {}", db_list.is_mst_valid().unwrap());
    }
    println!("db_list.is_dense(): {}", db_list.is_dense().unwrap());
    println!(
        "db_list.depth_of_node_tree(): {}",
        db_list.depth_of_node_tree().unwrap()
    );
    #[cfg(feature = "buf_stats")]
    println!("db_list.buf_stats(): {:?}", db_list.buf_stats());
    println!(
        "record_size_stats(): {}",
        db_list.record_size_stats().unwrap()
    );
}

fn _print_depth_db_map(db_map: &FileDbMap) {
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
}

fn _print_depth_db_list(db_list: &FileDbList) {
    println!(
        "db_list.depth_of_node_tree(): {}",
        db_list.depth_of_node_tree().unwrap()
    );
}
