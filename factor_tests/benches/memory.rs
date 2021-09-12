use criterion::{
    async_executor::FuturesExecutor, criterion_group, criterion_main, BenchmarkId, Criterion,
};
use factor_tests::Todo;

use factordb::{
    query::{expr::Expr, select::Select},
    schema::{builtin::AttrTitle, AttributeDescriptor},
    Db, Id,
};

async fn select_single_with_title_eq(db: &Db) {
    let filter = Expr::eq(AttrTitle::expr(), 99_999.to_string());
    let select = Select::new().with_filter(filter).with_limit(1);

    let page = db.select(select).await.unwrap();
    assert_eq!(page.items.len(), 1);
}

fn bench_filtering(c: &mut Criterion) {
    let mem = factordb::backend::memory::MemoryDb::new();
    let db = factordb::Db::new(mem);

    futures::executor::block_on(async {
        factor_tests::apply_schema(&db).await.unwrap();

        for index in 0..100_000 {
            db.create_entity(Todo {
                id: Id::from_uuid(uuid::Uuid::from_u128(100_000 + index as u128)),
                title: index.to_string(),
                description: Some(index.to_string()),
                done: index % 2 == 0,
            })
            .await
            .unwrap();
        }
    });

    c.bench_with_input(
        BenchmarkId::new("select_single_with_title_eq", "default"),
        &(),
        |b, _s| {
            b.to_async(FuturesExecutor)
                .iter(|| select_single_with_title_eq(&db));
        },
    );
}

criterion_group!(benches, bench_filtering);
criterion_main!(benches);
