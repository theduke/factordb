use factordb::{data::ValueType, query::migrate::Migration, schema::AttributeSchema};

async fn custom() {
    let backend = factordb::backend::log::LogDb::open(
        factordb::backend::log::log_memory::MemoryLogStore::new(),
    )
    .await
    .unwrap();
    // let backend = factordb::backend::memory::MemoryDb::new();
    let db = factordb::Db::new(backend);

    db.migrate(
        Migration::new().attr_create(
            AttributeSchema::new("test", "unique", ValueType::String).with_unique(true),
        ),
    )
    .await
    .unwrap();
}

fn main() {
    std::env::set_var("RUST_LOG", "trace");
    tracing_subscriber::fmt::init();
    futures::executor::block_on(custom());
}
