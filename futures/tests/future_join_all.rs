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
