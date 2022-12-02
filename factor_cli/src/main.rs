use std::path::PathBuf;

use anyhow::{bail, Context};
use clap::Parser;

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    match args.command {
        Cmd::GenerateSchema(c) => c.run(),
    }
}

#[derive(clap::Parser)]
struct Args {
    #[clap(subcommand)]
    command: Cmd,
}

#[derive(clap::Subcommand)]
enum Cmd {
    GenerateSchema(CmdGenerateSchema),
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum CodegenLanguage {
    Typescript,
    Rust,
}

#[derive(clap::Parser)]
struct CmdGenerateSchema {
    /// The language to generate the schema for.
    #[clap(short, long, value_enum)]
    language: CodegenLanguage,

    /// Include the factordb builtin schema types.
    #[clap(long)]
    with_builtins: bool,

    /// The file to write the generated code to.
    /// If not provided the code is written to stdout.
    #[clap(short = 'o', long)]
    out_path: Option<PathBuf>,

    #[clap(long)]
    skip_resolve_namespaced: bool,

    /// The path to a schema file.
    schema_path: PathBuf,
}

impl CmdGenerateSchema {
    fn run(&self) -> Result<(), anyhow::Error> {
        let (code, extension) = match self.language {
            CodegenLanguage::Typescript => {
                todo!()
            }
            CodegenLanguage::Rust => {
                let code = factor_tools::rust::generate_schema_from_file(
                    &self.schema_path,
                    false,
                    self.skip_resolve_namespaced,
                )?;

                (code, "rs")
            }
        };

        if let Some(path) = &self.out_path {
            // Write to file.
            let out_extension = path
                .extension()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default();

            if out_extension != extension {
                bail!(
                    "Invalid out path {}: expected a .{} file extension",
                    path.display(),
                    extension,
                );
            }

            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("Could not create parent directory {}", parent.display())
                })?;
            }
            std::fs::write(path, code)
                .with_context(|| format!("Could not write code to file {}", path.display()))?;
            Ok(())
        } else {
            // Just print to stdout.
            print!("{code}");
            Ok(())
        }
    }
}
