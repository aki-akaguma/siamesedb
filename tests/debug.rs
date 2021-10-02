mod debug {
    use shamdb::memdb::{MemoryDb, MemoryDbList, MemoryDbMap};
    //use shamdb::ShamDb;
    //
    #[test]
    fn test_size_of() {
        //
        //assert_eq!(std::mem::size_of::<ShamDb>(), 4);
        //
        assert_eq!(std::mem::size_of::<MemoryDb>(), 8);
        assert_eq!(std::mem::size_of::<MemoryDbMap>(), 8);
        assert_eq!(std::mem::size_of::<MemoryDbList>(), 8);
    }
}
