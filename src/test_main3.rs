use siamesedb::filedb::CheckFileDbMap;
use siamesedb::filedb::FileDbMapDbString;
use siamesedb::{DbXxx, DbXxxBase};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let vec = load_fixtures_procs();
    test01(&args[1], &vec)?;
    Ok(())
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
            //if !a.is_empty() {
            {
                vec.push((a.to_string(), b[..(b.len() - 1)].to_string()));
            }
        }
        line.clear();
    }
    vec
}

fn test01(db_name: &str, data: &[(String, String)]) -> std::io::Result<()> {
    let _ = std::fs::remove_dir_all(db_name);
    let db = siamesedb::open_file(db_name).unwrap();
    let mut db_map = db.db_map_string("some_map1").unwrap();
    //
    for (k, v) in data {
        eprintln!("k:'{}', v:'{}'", k, v);
        db_map.put_string(k, v.as_str()).unwrap();
    }
    //
    db_map.sync_data().unwrap();
    //
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
