// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

#[cfg(test)]
use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use thiserror::Error;

const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(50);

static INTERRUPT_REQUESTED: OnceLock<Arc<AtomicBool>> = OnceLock::new();
#[cfg(test)]
thread_local! {
    static TEST_INTERRUPT_REQUESTED: Cell<bool> = const { Cell::new(false) };
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("interrupted by Ctrl-C")]
pub struct InterruptError;

pub fn arm_ctrl_c_handler() -> Result<(), ctrlc::Error> {
    let interrupt_requested = if let Some(interrupt_requested) = INTERRUPT_REQUESTED.get() {
        Arc::clone(interrupt_requested)
    } else {
        let interrupt_requested = Arc::new(AtomicBool::new(false));
        let handler_flag = Arc::clone(&interrupt_requested);
        ctrlc::set_handler(move || {
            handler_flag.store(true, Ordering::SeqCst);
        })?;
        let _ = INTERRUPT_REQUESTED.set(Arc::clone(&interrupt_requested));
        interrupt_requested
    };
    interrupt_requested.store(false, Ordering::SeqCst);
    Ok(())
}

pub fn is_interrupt_requested() -> bool {
    #[cfg(test)]
    if TEST_INTERRUPT_REQUESTED.with(Cell::get) {
        return true;
    }
    INTERRUPT_REQUESTED
        .get()
        .is_some_and(|interrupt_requested| interrupt_requested.load(Ordering::SeqCst))
}

pub fn check_for_interrupt() -> Result<(), InterruptError> {
    if is_interrupt_requested() {
        Err(InterruptError)
    } else {
        Ok(())
    }
}

pub async fn wait_for_interrupt() -> InterruptError {
    #[cfg(not(test))]
    if INTERRUPT_REQUESTED.get().is_none() {
        std::future::pending::<()>().await;
        unreachable!("pending future does not resolve");
    }

    loop {
        if is_interrupt_requested() {
            return InterruptError;
        }
        tokio::time::sleep(INTERRUPT_POLL_INTERVAL).await;
    }
}

pub async fn run_until_interrupt<Fut, T>(operation: Fut) -> Result<T, InterruptError>
where
    Fut: Future<Output = T>,
{
    tokio::pin!(operation);
    let interrupt = wait_for_interrupt();
    tokio::pin!(interrupt);
    tokio::select! {
        result = &mut operation => Ok(result),
        interrupted = &mut interrupt => Err(interrupted),
    }
}

#[cfg(test)]
pub fn set_interrupt_requested_for_tests(requested: bool) {
    TEST_INTERRUPT_REQUESTED.with(|interrupt_requested| interrupt_requested.set(requested));
}
