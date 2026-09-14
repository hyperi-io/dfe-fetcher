// Project:   dfe-fetcher
// File:      crates/db/src/pump.rs
// Purpose:   A blocking driver cursor turned into a bounded async stream
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The blocking-cursor pump.
//!
//! ODBC cursors are synchronous. The pump runs the cursor on the blocking pool
//! and hands each fetched block over a BOUNDED channel to the async side; when
//! the driver stops polling, the channel fills, `blocking_send` parks the pump
//! and the cursor stops fetching. Memory is at most `capacity` blocks plus the
//! one in flight. A block is whatever the engine fetches per round trip (an
//! NDJSON buffer from `arrow-json`), never a whole result set.

use futures::stream::BoxStream;
use tokio::sync::mpsc;

use dfe_fetcher_core::error::{Error, Result};

/// Run `produce` on the blocking pool, yielding the blocks it emits as a
/// bounded stream. `produce` is handed a sender it calls once per block and
/// returns when the cursor is exhausted; an `Err` return ends the stream with
/// that error after the blocks already sent.
///
/// `capacity` is the number of blocks the async side may hold before the
/// producer parks; the survey's pick is 2.
///
/// SHORTCUT: one blocking-pool thread per active DB store; odbc-api's async
/// statement polling lifts that once active stores number in the hundreds.
#[must_use]
pub fn pump<T, F>(capacity: usize, produce: F) -> BoxStream<'static, Result<T>>
where
    T: Send + 'static,
    F: FnOnce(&mut dyn FnMut(T) -> bool) -> Result<()> + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<T>>(capacity.max(1));
    tokio::task::spawn_blocking(move || {
        let sender = tx.clone();
        let mut emit = move |block: T| sender.blocking_send(Ok(block)).is_ok();
        if let Err(e) = produce(&mut emit) {
            // A closed receiver means the driver went away; nobody is left to
            // read the error and the pump just stops.
            let _ = tx.blocking_send(Err(e));
        }
    });
    Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

/// Map a driver's SQLSTATE class onto the framework's error vocabulary: a
/// connection-class state is a `network` failure, an authorisation state a
/// `4xx`, anything else a `source` error carrying the text.
#[must_use]
pub fn classify_sqlstate(state: &str, text: &str) -> Error {
    match state.get(..2) {
        Some("08") => Error::Source(format!("connection ({state}): {text}")),
        Some("28") => Error::Credential(format!("authorisation ({state}): {text}")),
        Some("HY") if state == "HYT00" || state == "HYT01" => {
            Error::Source(format!("timeout ({state}): {text}"))
        }
        _ => Error::Source(format!("sql ({state}): {text}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocks_arrive_in_order_and_the_stream_ends_when_the_producer_returns() {
        let mut stream = pump(2, |emit| {
            for i in 0..5u32 {
                assert!(emit(i));
            }
            Ok(())
        });
        let mut seen = Vec::new();
        while let Some(block) = stream.next().await {
            seen.push(block.unwrap());
        }
        assert_eq!(seen, [0, 1, 2, 3, 4]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_producer_error_arrives_after_the_blocks_it_sent() {
        let mut stream = pump(1, |emit| {
            emit(1u32);
            Err(Error::Source("cursor broke".into()))
        });
        assert_eq!(stream.next().await.unwrap().unwrap(), 1);
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("cursor broke"));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_producer_parks_when_the_consumer_stops_polling() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        let produced = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&produced);
        let mut stream = pump(2, move |emit| {
            for i in 0..100u32 {
                counter.store(i + 1, Ordering::SeqCst);
                if !emit(i) {
                    break;
                }
            }
            Ok(())
        });
        assert_eq!(stream.next().await.unwrap().unwrap(), 0);
        // The pump can be at most capacity + the one blocked send ahead of us.
        tokio::task::yield_now().await;
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            produced.load(Ordering::SeqCst) <= 1 + 2 + 1,
            "produced {} blocks with only one consumed",
            produced.load(Ordering::SeqCst)
        );
        drop(stream);
    }

    /// Dropping the stream ends the producer: the next `emit` answers false,
    /// the closure returns, and the connection and statement it holds go with
    /// it. A cancelled tick would otherwise leave a cursor open on the engine
    /// for as long as the process lives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_stream_ends_the_producer_and_releases_what_it_held() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        use tokio::sync::Notify;

        let produced = Arc::new(AtomicU32::new(0));
        let returned = Arc::new(Notify::new());
        let counter = Arc::clone(&produced);
        let done = Arc::clone(&returned);
        let mut stream = pump(2, move |emit| {
            for i in 0..10_000u32 {
                counter.fetch_add(1, Ordering::SeqCst);
                if !emit(i) {
                    break;
                }
            }
            // Reached only when the producer returns, which is the statement
            // being released on the blocking thread.
            done.notify_one();
            Ok(())
        });
        assert_eq!(stream.next().await.unwrap().unwrap(), 0);
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(5), returned.notified())
            .await
            .expect("the producer returns once the consumer is gone");
        let settled = produced.load(Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            produced.load(Ordering::SeqCst),
            settled,
            "nothing is fetched after the stream is dropped"
        );
        assert!(settled < 10_000, "the producer stopped early: {settled}");
    }

    #[test]
    fn sqlstate_classes_map_to_the_error_vocabulary() {
        assert_eq!(
            classify_sqlstate("08001", "refused").api_error_code(),
            "network"
        );
        assert!(matches!(
            classify_sqlstate("28000", "denied"),
            Error::Credential(_)
        ));
        assert_eq!(
            classify_sqlstate("HYT00", "slow").api_error_code(),
            "timeout"
        );
        assert!(matches!(
            classify_sqlstate("42S02", "no table"),
            Error::Source(_)
        ));
    }
}
