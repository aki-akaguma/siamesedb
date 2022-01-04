use siamesedb::filedb::CheckFileDbMap;
use siamesedb::filedb::FileDbMapDbInt;
use siamesedb::filedb::FileDbMapString;
use siamesedb::DbMap;
use siamesedb::DbXxx;

fn main() {
    _test_a1();
    //_test_a2();
}
fn _test_a1() {
    //_test00_map_iter();
    _test00_map_iter2();
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
        CheckC { check: true, f_mst: true, ..Default::default() },
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
    finish put: 9.634µs/op
    record free: [(16, 0), (24, 0), (32, 0), (48, 0), (64, 0), (256, 0), (512, 0), (2048, 0)]
    record used: [(16, 100), (24, 999900), (32, 0), (48, 0), (64, 0), (256, 0), (512, 0), (2048, 0)]
    node free: [(32, 524), (72, 0), (104, 0), (144, 0), (176, 0), (216, 0), (232, 0), (256, 0)]
    node used: [(32, 0), (72, 138135), (104, 20984), (144, 0), (176, 0), (216, 0), (232, 0), (256, 0)]
    db_map.is_balanced(): true
    db_map.is_dense(): true
    db_map.depth_of_node_tree(): 7
    record_size_stats(): [(16, 100), (24, 999900)]
    start get
    finish get: 4.097µs/op
    start delete
    finish delete: 7.555µs/op
    record free: [(16, 100), (24, 999900), (32, 0), (48, 0), (64, 0), (256, 0), (512, 0), (2048, 0)]
    record used: [(16, 0), (24, 0), (32, 0), (48, 0), (64, 0), (256, 0), (512, 0), (2048, 0)]
    node free: [(32, 524), (72, 138134), (104, 20984), (144, 0), (176, 0), (216, 0), (232, 0), (256, 0)]
    node used: [(32, 0), (72, 1), (104, 0), (144, 0), (176, 0), (216, 0), (232, 0), (256, 0)]
    db_map.is_balanced(): true
    db_map.is_dense(): true
    db_map.depth_of_node_tree(): 1
    record_size_stats(): []
    21.65user 0.82system 0:23.24elapsed 96%CPU (0avgtext+0avgdata 2792maxresident)k
    672inputs+141216outputs (4major+353minor)pagefaults 0swaps
    0
    */
    /*
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 1_000_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
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
    /*
    finish put: 7.264µs/op
    db_map.depth_of_node_tree(): 5
    finish get: 3.181µs/op
    finish delete: 5.513µs/op
    0.15user 0.00system 0:00.17elapsed 93%CPU (0avgtext+0avgdata 2508maxresident)k
    */
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
    /*
    finish put: 8.305µs/op
    db_map.depth_of_node_tree(): 6
    finish get: 3.481µs/op
    finish delete: 6.592µs/op
    1.76user 0.06system 0:01.90elapsed 96%CPU (0avgtext+0avgdata 2900maxresident)k
    */
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
    /*
    finish put: 9.627µs/op
    db_map.depth_of_node_tree(): 7
    finish get: 4.082µs/op
    finish delete: 7.704µs/op
    20.52user 0.62system 0:21.76elapsed 97%CPU (0avgtext+0avgdata 3016maxresident)k
    */
    #[rustfmt::skip]
    _test_db_map(
        TestC { max_cnt: 1_000_000, f_get: true, f_delete: true, ..Default::default() },
        CheckC { check: false, f_depth: true, ..Default::default() },
        |i: usize| { format!("key{:01}", i) },
        |i: usize| { format!("value{:01}", i) },
    );
    /*
     */
    //
    // 10m
    //
    /*
    finish put: 10.951µs/op
    db_map.depth_of_node_tree(): 8
    finish get: 4.497µs/op
    finish delete: 9.281µs/op
    237.03user 7.59system 4:07.71elapsed 98%CPU (0avgtext+0avgdata 2856maxresident)k
    */
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
    // record free: [(16, 0), (24, 0), (32, 0), (48, 0), (64, 0), (256, 0), (512, 0), (2048, 0)]
    // record used: [(16, 100), (24, 999900), (32, 99000000), (48, 0), (64, 0), (256, 0), (512, 0), (2048, 0)]
    // node free: [(32, 503), (72, 1), (104, 0), (144, 0), (176, 0), (216, 0), (232, 0), (256, 0)]
    // node used: [(32, 0), (72, 8605028), (104, 5307029), (144, 2000655), (176, 0), (216, 0), (232, 0), (256, 0)]
    // db_map.is_balanced(): true
    // db_map.is_dense(): true
    // record_size_stats(): [(16, 100), (24, 999900), (32, 99000000)]
    //
    /*
    finish put: 12.99µs/op
    db_map.depth_of_node_tree(): 9
    finish get: 8.188µs/op
    finish delete: 18.575µs/op
    2706.34user 186.77system 1:06:16elapsed 72%CPU (0avgtext+0avgdata 2880maxresident)k
    */
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
    let db_name = "target/tmp/testA.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    {
        db_map.put_string("key1".into(), "value1").unwrap();
        db_map.put_string("key2".into(), "value2").unwrap();
        db_map.put_string("key3".into(), "value3").unwrap();
        db_map.put_string("key4".into(), "value4").unwrap();
        db_map.put_string("key5".into(), "value5").unwrap();
        //
        db_map.put_string("key6".into(), "value6").unwrap();
        db_map.put_string("key7".into(), "value7").unwrap();
        db_map.put_string("key8".into(), "value8").unwrap();
        db_map.put_string("key9".into(), "value9").unwrap();
        //
        db_map.sync_data().unwrap();
    }
    //
    //println!("{}", db_map.graph_string().unwrap());
    println!("{}", db_map.graph_string_with_key_string().unwrap());
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
    //println!("{}", db_map.graph_string().unwrap());
    println!("{}", db_map.graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
}

fn _test00_map_iter() {
    let db_name = "target/tmp/testA.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    {
        // insert
        db_map.put_string("key01".into(), "value1").unwrap();
        db_map.put_string("key02".into(), "value2").unwrap();
        db_map.put_string("key03".into(), "value3").unwrap();
        db_map.put_string("key04".into(), "value4").unwrap();
        db_map.put_string("key05".into(), "value5").unwrap();
        db_map.put_string("key06".into(), "value6").unwrap();
        db_map.put_string("key07".into(), "value7").unwrap();
        db_map.put_string("key08".into(), "value8").unwrap();
        db_map.put_string("key09".into(), "value9").unwrap();
        db_map.put_string("key10".into(), "value10").unwrap();
        db_map.put_string("key11".into(), "value11").unwrap();
        db_map.put_string("key12".into(), "value12").unwrap();
        db_map.put_string("key13".into(), "value13").unwrap();
        db_map.put_string("key14".into(), "value14").unwrap();
        db_map.put_string("key15".into(), "value15").unwrap();
        db_map.put_string("key16".into(), "value16").unwrap();
        db_map.put_string("key17".into(), "value17").unwrap();
        db_map.put_string("key18".into(), "value18").unwrap();
        //
        println!("{}", db_map.graph_string_with_key_string().unwrap());
        //
        // iterator
        let mut iter = db_map.iter_mut();
        assert_eq!(iter.next(), Some(("key01".into(), b"value1".to_vec())));
        assert_eq!(iter.next(), Some(("key02".into(), b"value2".to_vec())));
        assert_eq!(iter.next(), Some(("key03".into(), b"value3".to_vec())));
        assert_eq!(iter.next(), Some(("key04".into(), b"value4".to_vec())));
        assert_eq!(iter.next(), Some(("key05".into(), b"value5".to_vec())));
        //
        assert_eq!(iter.next(), Some(("key06".into(), b"value6".to_vec())));
        //
        assert_eq!(iter.next(), Some(("key07".into(), b"value7".to_vec())));
        assert_eq!(iter.next(), Some(("key08".into(), b"value8".to_vec())));
        assert_eq!(iter.next(), Some(("key09".into(), b"value9".to_vec())));
        assert_eq!(iter.next(), Some(("key10".into(), b"value10".to_vec())));
        assert_eq!(iter.next(), Some(("key11".into(), b"value11".to_vec())));
        //
        assert_eq!(iter.next(), Some(("key12".into(), b"value12".to_vec())));
        //
        assert_eq!(iter.next(), Some(("key13".into(), b"value13".to_vec())));
        assert_eq!(iter.next(), Some(("key14".into(), b"value14".to_vec())));
        assert_eq!(iter.next(), Some(("key15".into(), b"value15".to_vec())));
        assert_eq!(iter.next(), Some(("key16".into(), b"value16".to_vec())));
        assert_eq!(iter.next(), Some(("key17".into(), b"value17".to_vec())));
        assert_eq!(iter.next(), Some(("key18".into(), b"value18".to_vec())));
        assert_eq!(iter.next(), None);
        //
        db_map.sync_data().unwrap();
    }
}

fn _test00_map_iter2() {
    let db_name = "target/tmp/testA.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    {
        // insert
        for i in 0..100 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            db_map.put_string(key.into(), &value).unwrap();
        }
        //
        println!("{}", db_map.graph_string_with_key_string().unwrap());
        //
        // iterator
        let mut iter = db_map.iter_mut();
        for i in 0..100 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            assert_eq!(iter.next(), Some((key.into(), value.as_bytes().to_vec())));
        }
        assert_eq!(iter.next(), None);
        //
        db_map.sync_data().unwrap();
    }
}

fn _test00_list() {
    let db_name = "target/tmp/testA.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_list = db.db_map_int("some_list1").unwrap();
    //
    {
        db_list.put_string(1.into(), "value1").unwrap();
        db_list.put_string(2.into(), "value2").unwrap();
        db_list.put_string(3.into(), "value3").unwrap();
        db_list.put_string(4.into(), "value4").unwrap();
        db_list.put_string(5.into(), "value5").unwrap();
        //
        db_list.put_string(6.into(), "value6").unwrap();
        db_list.put_string(7.into(), "value7").unwrap();
        db_list.put_string(8.into(), "value8").unwrap();
        db_list.put_string(9.into(), "value9").unwrap();
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
    //println!("{}", db_list.graph_string().unwrap());
    println!("{}", db_list.graph_string_with_key_string().unwrap());
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
        db_list.delete(&4).unwrap();
        //db_map.delete("key5").unwrap();
        //db_map.delete("key6").unwrap();
        //db_map.delete("key7").unwrap();
        //db_map.delete("key8").unwrap();
        //db_map.delete("key9").unwrap();
        //
        db_list.sync_data().unwrap();
    }
    //
    //println!("{}", db_list.graph_string().unwrap());
    println!("{}", db_list.graph_string_with_key_string().unwrap());
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
    let db_name = "target/tmp/testA.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, None);
    //
    db_map.put_string("key1".into(), "value1").unwrap();
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value1".to_string()));
    //
    db_map.put_string("key2".into(), "value2").unwrap();
    let r = db_map.get_string("key2").unwrap();
    assert_eq!(r, Some("value2".to_string()));
    //
    db_map.put_string("key0".into(), "value0").unwrap();
    let r = db_map.get_string("key0").unwrap();
    assert_eq!(r, Some("value0".to_string()));
    //
    println!("{}", db_map.graph_string_with_key_string().unwrap());
    db_map.put_string("key1".into(), "value2").unwrap();
    println!("{}", db_map.graph_string_with_key_string().unwrap());
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value2".to_string()));
    //
    db_map.put_string("key1".into(), "value99").unwrap();
    let r = db_map.get_string("key1").unwrap();
    assert_eq!(r, Some("value99".to_string()));
    //
    db_map.sync_data().unwrap();
    //
    //println!("{}", db_map.graph_string().unwrap());
    println!("{}", db_map.graph_string_with_key_string().unwrap());
    println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
    println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
}

fn _test02() {
    let db_name = "target/tmp/testA.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    for i in 1..20 {
        let key = format!("key{:02}", i);
        let val = format!("value{:02}", i);
        db_map.put_string(key.into(), &val).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
    println!("{}", db_map.graph_string_with_key_string().unwrap());
    println!(
        "key record free: {:?}",
        db_map.count_of_free_key_record().unwrap()
    );
    println!(
        "value record free: {:?}",
        db_map.count_of_free_value_record().unwrap()
    );
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
    //println!("{}", db_map.graph_string().unwrap());
    println!("{}", db_map.graph_string_with_key_string().unwrap());
    println!(
        "key record free: {:?}",
        db_map.count_of_free_key_record().unwrap()
    );
    println!(
        "value record free: {:?}",
        db_map.count_of_free_value_record().unwrap()
    );
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

#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
struct TestC {
    max_cnt: usize,
    f_get: bool,
    f_delete: bool,
    f_repeat: usize,
}

#[allow(dead_code)]
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
    let db_name = "target/tmp/testA.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    for _ in 0..test_cnf.f_repeat.max(1) {
        println!("start put");
        let instant_st = std::time::Instant::now();
        for i in 0..test_cnf.max_cnt {
            let key = fmt_key_func(i);
            let val = fmt_val_func(i);
            db_map.put_string(key.into(), &val).unwrap();
        }
        let instant_ed = std::time::Instant::now();
        let instant_per_op = (instant_ed - instant_st) / test_cnf.max_cnt.try_into().unwrap();
        println!("finish put: {:?}/op", instant_per_op);
        //
        db_map.flush().unwrap();
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
            db_map.flush().unwrap();
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
    let db_name = "target/tmp/testA.siamesedb";
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_list = db.db_map_int("some_list1").unwrap();
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
            db_list.put_string(key.into(), &val).unwrap();
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
                let s = db_list.get_string(&key).unwrap();
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
                db_list.delete(&key).unwrap();
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

fn _print_check_db_map(db_map: &FileDbMapString, check_cnf: CheckC) {
    if check_cnf.f_graph {
        println!("{}", db_map.graph_string_with_key_string().unwrap());
    }
    println!(
        "key record free: {:?}",
        db_map.count_of_free_key_record().unwrap()
    );
    let (key_rec_v, val_rec_v, node_v) = db_map.count_of_used_node().unwrap();
    println!("key record used: {:?}", key_rec_v);
    println!(
        "value record free: {:?}",
        db_map.count_of_free_value_record().unwrap()
    );
    println!("value record used: {:?}", val_rec_v);
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
        "key_record_size_stats(): {}",
        db_map.key_record_size_stats().unwrap()
    );
    println!(
        "value_record_size_stats(): {}",
        db_map.value_record_size_stats().unwrap()
    );
}

fn _print_check_db_list(db_map: &FileDbMapDbInt, check_cnf: CheckC) {
    if check_cnf.f_graph {
        println!("{}", db_map.graph_string_with_key_string().unwrap());
    }
    println!(
        "key record free: {:?}",
        db_map.count_of_free_key_record().unwrap()
    );
    let (key_rec_v, val_rec_v, node_v) = db_map.count_of_used_node().unwrap();
    println!("key record used: {:?}", key_rec_v);
    println!(
        "value record free: {:?}",
        db_map.count_of_free_value_record().unwrap()
    );
    println!("value record used: {:?}", val_rec_v);
    println!("node free: {:?}", db_map.count_of_free_node().unwrap());
    println!("node used: {:?}", node_v);
    println!("db_list.is_balanced(): {}", db_map.is_balanced().unwrap());
    if check_cnf.f_mst {
        println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    }
    println!("db_list.is_dense(): {}", db_map.is_dense().unwrap());
    println!(
        "db_list.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
    #[cfg(feature = "buf_stats")]
    println!("db_list.buf_stats(): {:?}", db_map.buf_stats());
    println!(
        "key_record_size_stats(): {}",
        db_map.key_record_size_stats().unwrap()
    );
    println!(
        "value_record_size_stats(): {}",
        db_map.value_record_size_stats().unwrap()
    );
}

fn _print_depth_db_map(db_map: &FileDbMapString) {
    println!(
        "db_map.depth_of_node_tree(): {}",
        db_map.depth_of_node_tree().unwrap()
    );
}

fn _print_depth_db_list(db_list: &FileDbMapDbInt) {
    println!(
        "db_list.depth_of_node_tree(): {}",
        db_list.depth_of_node_tree().unwrap()
    );
}
