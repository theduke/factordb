use factordb::{Attribute, Db, Migration, ValueType};

async fn custom() {
    let backend = factor_engine::backend::log::LogDb::open(
        factor_engine::backend::log::store_memory::MemoryLogStore::new(),
    )
    .await
    .unwrap();
    let engine = factor_engine::Engine::new(backend);
    // let backend = factordb::backend::memory::MemoryDb::new();
    let db = Db::new(engine);

    db.migrate(
        Migration::new()
            .attr_create(Attribute::new("test/unique", ValueType::String).with_unique(true)),
    )
    .await
    .unwrap();
}

fn main() {
    std::env::set_var("RUST_LOG", "trace");
    tracing_subscriber::fmt::init();
    futures::executor::block_on(custom());
}
