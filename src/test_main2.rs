use siamesedb::filedb::CheckFileDbMap;
use siamesedb::filedb::FileDbMapDbString;
use siamesedb::{DbXxx, DbXxxBase};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    test01(&args[1])?;
    Ok(())
}

fn test01(db_name: &str) -> std::io::Result<()> {
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    let val = "ab".repeat(1024 * 1024 * 1024);
    //
    db_map.put_string("A", &val).unwrap();
    db_map.put_string("B", &val).unwrap();
    //
    db_map.sync_data().unwrap();
    _print_check_db_map(
        &db_map,
        CheckC {
            _check: true,
            ..Default::default()
        },
    );
    //
    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
struct CheckC {
    _check: bool,
    f_mst: bool,
    f_graph: bool,
}

fn _print_check_db_map(db_map: &FileDbMapDbString, check_cnf: CheckC) {
    if check_cnf.f_graph {
        println!("{}", db_map.graph_string_with_key_string().unwrap());
    }
    println!(
        "key piece free: {:?}",
        db_map.count_of_free_key_piece().unwrap()
    );
    let (key_rec_v, val_rec_v, node_v) = db_map.count_of_used_node().unwrap();
    println!("key piece used: {:?}", key_rec_v);
    println!(
        "value piece free: {:?}",
        db_map.count_of_free_value_piece().unwrap()
    );
    println!("value piece used: {:?}", val_rec_v);
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
        "key_piece_size_stats(): {}",
        db_map.key_piece_size_stats().unwrap()
    );
    println!(
        "value_piece_size_stats(): {}",
        db_map.value_piece_size_stats().unwrap()
    );
}
