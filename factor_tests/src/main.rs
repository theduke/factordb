use factor_engine::{backend::memory::MemoryDb, Engine};

use factor_tests::{apply_schema, select_single_todo_with_title_eq, Todo};

async fn bench_select_single() {
    let db = Engine::new(MemoryDb::new()).into_client();
    apply_schema(&db).await.unwrap();

    for index in 0..10_000 {
        db.create_entity(Todo::new_from_index(index)).await.unwrap();
    }

    let title = 9_999.to_string();

    let iterations = 100_000;
    eprintln!("All items created, starting selects");
    for index in 0..iterations {
        if index % 10_000 == 0 {
            eprintln!("{}/{}", index, iterations);
        }
        select_single_todo_with_title_eq(&db, title.clone())
            .await
            .unwrap();
    }
}

fn main() {
    futures::executor::block_on(bench_select_single());
}
