fn main() -> Result<(), String> {
    let args = std::env::args().collect::<Vec<_>>();
    let args_ref = args.iter().map(|s| s.as_str()).collect::<Vec<_>>();

    match args_ref.as_slice() {
        &["rust", schema_path] => {
            let code = factor_tools::rust::generate_schema_from_file(schema_path, true).unwrap();
            print!("{code}");
            Ok(())
        }
        other => Err(format!("unexpected args: {:?}", other)),
    }
}
