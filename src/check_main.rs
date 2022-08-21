use siamesedb::filedb::{CheckFileDbMap, FileBufSizeParam, FileDbParams};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "-s" => check_string(&args[2])?,
        "-b" => check_bytes(&args[2])?,
        "-u" => check_dbint(&args[2])?,
        _ => {
            eprintln!("[usage] {} {{-s|-b|-u}} path", args[0]);
        }
    }
    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
struct CheckC {
    check: bool,
    f_mst: bool,
    f_graph: bool,
}

fn check_string(db_name: &str) -> std::io::Result<()> {
    let db = siamesedb::open_file(db_name).unwrap();
    let db_map = db
        .db_map_string_with_params(
            "some_map1",
            FileDbParams {
                key_buf_size: FileBufSizeParam::PerMille(1000),
                idx_buf_size: FileBufSizeParam::PerMille(1000),
                ..Default::default()
            },
        )
        .unwrap();
    _print_check_db_map(
        &db_map,
        CheckC {
            check: true,
            f_mst: true,
            f_graph: false,
            /*
            check: false,
            f_mst: false,
            f_graph: true,
            */
        },
    );
    //
    Ok(())
}

fn check_bytes(db_name: &str) -> std::io::Result<()> {
    let db = siamesedb::open_file(db_name).unwrap();
    let db_map = db
        .db_map_bytes_with_params(
            "some_map1",
            FileDbParams {
                key_buf_size: FileBufSizeParam::PerMille(1000),
                idx_buf_size: FileBufSizeParam::PerMille(1000),
                ..Default::default()
            },
        )
        .unwrap();
    _print_check_db_map(
        &db_map,
        CheckC {
            check: true,
            f_mst: true,
            f_graph: false,
            /*
            check: false,
            f_mst: false,
            f_graph: true,
            */
        },
    );
    //
    Ok(())
}

fn check_dbint(db_name: &str) -> std::io::Result<()> {
    let db = siamesedb::open_file(db_name).unwrap();
    let db_map = db
        .db_map_int_with_params(
            "some_map1",
            FileDbParams {
                key_buf_size: FileBufSizeParam::PerMille(1000),
                idx_buf_size: FileBufSizeParam::PerMille(1000),
                ..Default::default()
            },
        )
        .unwrap();
    _print_check_db_map(
        &db_map,
        CheckC {
            check: true,
            f_mst: true,
            f_graph: false,
            /*
            check: false,
            f_mst: false,
            f_graph: true,
            */
        },
    );
    //
    Ok(())
}

fn _print_check_db_map(db_map: &dyn CheckFileDbMap, check_cnf: CheckC) {
    if check_cnf.f_graph {
        println!("{}", db_map.graph_string_with_key_string().unwrap());
    }
    if check_cnf.check {
        #[cfg(feature = "htx")]
        {
            let (ht_size, count) = db_map.ht_size_and_count().unwrap();
            println!("count / ht: {}/{}", count, ht_size);
        }
        //
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
        //
        println!(
            "db_map.depth_of_node_tree(): {}",
            db_map.depth_of_node_tree().unwrap()
        );
        println!("db_map.is_balanced(): {}", db_map.is_balanced().unwrap());
        println!("db_map.is_dense(): {}", db_map.is_dense().unwrap());
        #[cfg(feature = "buf_stats")]
        println!("db_map.buf_stats(): {:?}", db_map.buf_stats());
        //
        println!(
            "key_piece_size_stats(): {}",
            db_map.key_piece_size_stats().unwrap()
        );
        println!(
            "value_piece_size_stats(): {}",
            db_map.value_piece_size_stats().unwrap()
        );
        println!("keys_count_stats(): {}", db_map.keys_count_stats().unwrap());
        println!("key_length_stats(): {}", db_map.key_length_stats().unwrap());
        println!(
            "value_length_stats(): {}",
            db_map.value_length_stats().unwrap()
        );
        #[cfg(feature = "htx")]
        {
            let (count, per_mill) = db_map.htx_filling_rate_per_mill().unwrap();
            println!(
                "htx_filling_rate(): {:.2}%, count: {}",
                per_mill as f64 / 10.0,
                count
            );
        }
    }
    if check_cnf.f_mst {
        println!("db_map.is_mst_valid(): {}", db_map.is_mst_valid().unwrap());
    }
}
