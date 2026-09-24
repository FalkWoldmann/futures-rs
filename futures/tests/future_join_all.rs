use std::{
    fmt::Debug,
    pin::{Pin, pin},
};

use futures::{
    executor::block_on,
    future::{Future, JoinAll, join_all, ready},
};

#[track_caller]
fn assert_done<T>(actual_fut: impl Future<Output = T>, expected: T)
where
    T: PartialEq + Debug,
{
    let output = block_on(pin!(actual_fut));
    assert_eq!(output, expected);
}

#[test]
fn collect_collects() {
    assert_done(join_all(vec![ready(1), ready(2)]), vec![1, 2]);
    assert_done(join_all(vec![ready(1)]), vec![1]);
    // REVIEW: should this be implemented?
    // assert_done(join_all(Vec::<i32>::new()), vec![]);

    // TODO: needs more tests
}

#[test]
fn join_all_iter_lifetime() {
    // In futures-rs version 0.1, this function would fail to typecheck due to an overly
    // conservative type parameterization of `JoinAll`.
    fn sizes(bufs: Vec<&[u8]>) -> impl Future<Output = Vec<usize>> {
        let iter = bufs.into_iter().map(|b| ready::<usize>(b.len()));
        join_all(iter)
    }

    assert_done(sizes(vec![&[1, 2, 3], &[], &[0]]), vec![3_usize, 0, 1]);
}

#[test]
fn join_all_from_iter() {
    assert_done(vec![ready(1), ready(2)].into_iter().collect::<JoinAll<_>>(), vec![1, 2])
}

#[test]
fn join_all_preserves_order() {
    use futures::{channel::oneshot, task::Poll};
    use futures_test::task::noop_context;

    for n in [0, 1, 30, 31, 100] {
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..n).map(|_| oneshot::channel()).unzip();
        let mut fut = join_all(rxs);
        if n > 0 {
            assert!(Pin::new(&mut fut).poll(&mut noop_context()).is_pending());
        }
        // Complete the futures in reverse order.
        for (i, tx) in txs.into_iter().enumerate().rev() {
            tx.send(i).unwrap();
        }
        let expected: Vec<_> = (0..n).map(Ok).collect();
        assert_eq!(Pin::new(&mut fut).poll(&mut noop_context()), Poll::Ready(expected));
    }
}

#[test]
fn join_all_unbounded_size_hint() {
    for n in [0, 1, 30, 31, 100] {
        let iter = (0..).take_while(|&i| i < n).map(ready);
        assert_eq!(iter.size_hint(), (0, None));
        assert_done(join_all(iter), (0..n).collect::<Vec<_>>());
    }
}

/// A future that completes on its `n`-th poll. Before that it stores a clone of
/// its waker in `wakers`, wakes itself if `self_wake` is set, and returns
/// `Pending`.
struct Countdown {
    n: usize,
    self_wake: bool,
    wakers: std::sync::Arc<std::sync::Mutex<Vec<std::task::Waker>>>,
}

impl Future for Countdown {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<usize> {
        self.n -= 1;
        if self.n == 0 {
            return std::task::Poll::Ready(0);
        }
        self.wakers.lock().unwrap().push(cx.waker().clone());
        if self.self_wake {
            cx.waker().wake_by_ref();
        }
        std::task::Poll::Pending
    }
}

#[test]
fn join_all_wakers_outlive_future() {
    use std::sync::{Arc, Mutex};

    use futures_test::task::noop_context;

    let wakers = Arc::new(Mutex::new(Vec::new()));
    let futures = (0..100).map(|_| Countdown { n: 3, self_wake: false, wakers: wakers.clone() });
    let mut fut = join_all(futures);
    assert!(Pin::new(&mut fut).poll(&mut noop_context()).is_pending());
    // Wake only some of the futures, some of them twice.
    for waker in wakers.lock().unwrap().iter().step_by(3) {
        waker.wake_by_ref();
        waker.wake_by_ref();
    }
    assert!(Pin::new(&mut fut).poll(&mut noop_context()).is_pending());
    drop(fut);
    // Wake and drop the wakers after the future is gone.
    let wakers = std::mem::take(&mut *wakers.lock().unwrap());
    for (i, waker) in wakers.into_iter().enumerate() {
        if i % 2 == 0 {
            waker.wake();
        } else {
            waker.wake_by_ref();
        }
    }
}

#[test]
fn join_all_stale_wakeups() {
    use std::sync::{Arc, Mutex};

    // Futures that wake themselves on every poll, including the ones where
    // they complete, and are woken again after they complete.
    let wakers = Arc::new(Mutex::new(Vec::new()));
    let futures =
        (0..100).map(|i| Countdown { n: 1 + i % 4, self_wake: true, wakers: wakers.clone() });
    let fut = join_all(futures);
    let wakers2 = wakers.clone();
    let fut = async move {
        let res = fut.await;
        for waker in wakers2.lock().unwrap().drain(..) {
            waker.wake();
        }
        res
    };
    assert_done(fut, vec![0; 100]);
}

#[test]
fn join_all_cross_thread_wakeups() {
    use futures::channel::oneshot;

    for n in [31, 1000] {
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..n).map(|_| oneshot::channel()).unzip();
        let handle = std::thread::spawn(move || {
            for (i, tx) in txs.into_iter().enumerate().rev() {
                tx.send(i).unwrap();
            }
        });
        assert_done(join_all(rxs), (0..n).map(Ok).collect::<Vec<_>>());
        handle.join().unwrap();
    }
}

#[test]
fn join_all_chain() {
    use futures::{FutureExt, channel::oneshot};

    // Future `i` completes after future `i - 1`, so each wake-up makes only
    // one future ready.
    let n = 200;
    let (txs, rxs): (Vec<_>, Vec<_>) = (0..n).map(|_| oneshot::channel::<usize>()).unzip();
    let mut txs: Vec<_> = txs.into_iter().map(Some).collect();
    let first = txs[0].take().unwrap();
    let futures: Vec<_> = rxs
        .into_iter()
        .enumerate()
        .map(|(i, rx)| {
            let next = txs.get_mut(i + 1).and_then(Option::take);
            rx.map(move |v| {
                let v = v.unwrap();
                if let Some(next) = next {
                    next.send(v + 1).unwrap();
                }
                v
            })
        })
        .collect();
    first.send(0).unwrap();
    assert_done(join_all(futures), (0..n).collect::<Vec<_>>());
}

#[test]
fn join_all_panic_in_poll() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use futures::FutureExt;
    use futures_test::task::noop_context;

    let futures = (0..100).map(|i| {
        async move {
            if i == 50 {
                panic!("boom");
            }
            futures::pending!();
            i
        }
        .boxed()
    });
    let mut fut = join_all(futures);
    let res = catch_unwind(AssertUnwindSafe(|| Pin::new(&mut fut).poll(&mut noop_context())));
    assert!(res.is_err());
    drop(fut);
}

#[test]
fn join_all_repoll_after_completion() {
    use futures_test::task::noop_context;

    for n in [3, 100] {
        let mut fut = join_all((0..n).map(ready));
        let cx = &mut noop_context();
        assert_eq!(Pin::new(&mut fut).poll(cx), std::task::Poll::Ready((0..n).collect()));
        assert_eq!(Pin::new(&mut fut).poll(cx), std::task::Poll::Ready(vec![]));
    }
}

#[test]
fn join_all_cross_thread_stress() {
    use futures::channel::oneshot;

    // Several threads complete the futures while `block_on` polls and parks,
    // so wake-ups race with the check for an empty queue.
    let rounds = if cfg!(miri) { 2 } else { 200 };
    let n = if cfg!(miri) { 40 } else { 400 };
    for _ in 0..rounds {
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..n).map(|_| oneshot::channel()).unzip();
        let mut txs: Vec<_> = txs.into_iter().enumerate().collect();
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let chunk: Vec<_> = txs.drain(..(n / (4 - t)).min(txs.len())).collect();
                std::thread::spawn(move || {
                    for (i, tx) in chunk {
                        tx.send(i).unwrap();
                    }
                })
            })
            .collect();
        assert_done(join_all(rxs), (0..n).map(Ok).collect::<Vec<_>>());
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
