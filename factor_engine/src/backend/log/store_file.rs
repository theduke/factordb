use std::path::PathBuf;

use futures::{
    future::{ready, BoxFuture},
    stream::BoxStream,
    FutureExt, StreamExt, TryStreamExt,
};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt};

use factordb::AnyError;

use super::{EventId, LogConverter, LogEvent};

/// Mock memory log store.
/// Only useful for testing.
pub struct FileLogStore<C> {
    converter: C,
    path: PathBuf,
    file: tokio::sync::Mutex<tokio::fs::File>,
}

impl<C: LogConverter> FileLogStore<C> {
    pub async fn open(converter: C, path: impl Into<PathBuf>) -> Result<Self, AnyError> {
        let path = path.into();

        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;

        file.seek(std::io::SeekFrom::End(0)).await?;

        Ok(Self {
            converter,
            path,
            file: tokio::sync::Mutex::new(file),
        })
    }
}

impl<C: LogConverter> super::LogStore for FileLogStore<C> {
    fn as_any(&self) -> &dyn std::any::Any {
        &*self
    }

    fn iter_events<'a>(
        &'a self,
        from: EventId,
        until: EventId,
    ) -> BoxFuture<'a, Result<BoxStream<'a, Result<LogEvent, AnyError>>, AnyError>> {
        let f = async move {
            let file = tokio::fs::File::open(&self.path).await?;
            let buf = tokio::io::BufReader::new(file);
            let lines = tokio_stream::wrappers::LinesStream::new(buf.lines());

            let stream = lines
                .map_err(AnyError::from)
                .and_then(move |line| async move {
                    let event = self.converter.clone().deserialize(line.as_bytes())?;
                    Ok(event)
                })
                .skip_while(move |res| {
                    let flag = if let Ok(ev) = &res {
                        ev.id < from
                    } else {
                        false
                    };
                    futures::future::ready(flag)
                })
                .take_while(move |res| {
                    let flag = if let Ok(ev) = &res {
                        ev.id <= until
                    } else {
                        false
                    };
                    futures::future::ready(flag)
                });

            Ok(stream.boxed())
        };
        f.boxed()
    }

    fn read_event(&self, _id: EventId) -> BoxFuture<Result<Option<LogEvent>, AnyError>> {
        std::future::ready(Err(anyhow::anyhow!("read_event not supported"))).boxed()
    }

    fn write_event<'a>(&'a mut self, event: LogEvent) -> BoxFuture<'a, Result<(), AnyError>> {
        async move {
            let mut converted = self.converter.serialize(&event)?;
            converted.push(b'\n');
            let mut file = self.file.lock().await;
            file.write_all(&converted).await?;
            file.flush().await?;

            Ok(())
        }
        .boxed()
    }

    fn clear<'a>(&'a mut self) -> BoxFuture<'a, Result<(), AnyError>> {
        async move {
            let mut file = self.file.lock().await;
            file.set_len(0).await?;
            file.seek(std::io::SeekFrom::Start(0)).await?;
            Ok(())
        }
        .boxed()
    }

    fn size_log(&mut self) -> BoxFuture<'static, Result<Option<u64>, AnyError>> {
        ready(Ok(None)).boxed()
    }

    fn size_data(&mut self) -> BoxFuture<'static, Result<Option<u64>, AnyError>> {
        ready(Ok(None)).boxed()
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::log::convert_json::JsonConverter;

    use super::*;

    #[test]
    fn test_backend_log_store_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let test_path = std::env::temp_dir().join("factordb_log_fs_backend_test.db");
        if test_path.is_file() {
            std::fs::remove_file(&test_path).unwrap();
        }

        let handle = rt.handle();

        let log = rt.block_on(async move {
            let fs = FileLogStore::open(JsonConverter, test_path).await.unwrap();
            let log = super::super::LogDb::open(fs).await.unwrap();
            log
        });
        crate::tests::test_backend(log, move |f| handle.block_on(f));
    }
}
