// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

#[cfg(test)]
use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use thiserror::Error;

const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const FORCED_INTERRUPT_EXIT_CODE: i32 = 130;

static INTERRUPT_REQUESTED: OnceLock<Arc<AtomicBool>> = OnceLock::new();
static INTERRUPT_SIGNAL_COUNT: AtomicUsize = AtomicUsize::new(0);
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
        ctrlc::set_handler(move || match next_interrupt_action(handler_flag.as_ref()) {
            InterruptAction::RequestGracefulShutdown => {}
            InterruptAction::ForceExit => {
                eprintln!("Second Ctrl-C received; forcing immediate exit.");
                std::process::exit(FORCED_INTERRUPT_EXIT_CODE);
            }
        })?;
        let _ = INTERRUPT_REQUESTED.set(Arc::clone(&interrupt_requested));
        interrupt_requested
    };
    INTERRUPT_SIGNAL_COUNT.store(0, Ordering::SeqCst);
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

enum InterruptAction {
    RequestGracefulShutdown,
    ForceExit,
}

fn next_interrupt_action(interrupt_requested: &AtomicBool) -> InterruptAction {
    if INTERRUPT_SIGNAL_COUNT.fetch_add(1, Ordering::SeqCst) == 0 {
        interrupt_requested.store(true, Ordering::SeqCst);
        InterruptAction::RequestGracefulShutdown
    } else {
        InterruptAction::ForceExit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    struct InterruptSignalCountTestGuard {
        _guard: MutexGuard<'static, ()>,
    }

    impl InterruptSignalCountTestGuard {
        fn new() -> Self {
            static INTERRUPT_SIGNAL_COUNT_TEST_MUTEX: Mutex<()> = Mutex::new(());
            let guard = INTERRUPT_SIGNAL_COUNT_TEST_MUTEX
                .lock()
                .expect("interrupt signal count test mutex poisoned");
            INTERRUPT_SIGNAL_COUNT.store(0, Ordering::SeqCst);
            Self { _guard: guard }
        }
    }

    impl Drop for InterruptSignalCountTestGuard {
        fn drop(&mut self) {
            INTERRUPT_SIGNAL_COUNT.store(0, Ordering::SeqCst);
        }
    }

    #[test]
    fn first_interrupt_requests_graceful_shutdown() {
        let _guard = InterruptSignalCountTestGuard::new();
        let interrupt_requested = AtomicBool::new(false);

        let action = next_interrupt_action(&interrupt_requested);

        assert!(matches!(action, InterruptAction::RequestGracefulShutdown));
        assert!(interrupt_requested.load(Ordering::SeqCst));
    }

    #[test]
    fn second_interrupt_forces_exit() {
        let _guard = InterruptSignalCountTestGuard::new();
        let interrupt_requested = AtomicBool::new(false);

        assert!(matches!(
            next_interrupt_action(&interrupt_requested),
            InterruptAction::RequestGracefulShutdown
        ));
        let action = next_interrupt_action(&interrupt_requested);

        assert!(matches!(action, InterruptAction::ForceExit));
    }
}
