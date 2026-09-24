use std::{fmt::Debug, pin::pin};

use futures::{
    executor::block_on,
    future::{Future, TryJoinAll, err, ok, ready, try_join_all},
};
use futures_test::future::FutureTestExt;

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
    assert_done(try_join_all(vec![ok(1), ok(2)]), Ok::<_, usize>(vec![1, 2]));
    assert_done(try_join_all(vec![ok(1), err(2)]), Err(2));
    assert_done(try_join_all(vec![ok(1)]), Ok::<_, usize>(vec![1]));
    // REVIEW: should this be implemented?
    // assert_done(try_join_all(Vec::<i32>::new()), Ok(vec![]));

    // TODO: needs more tests
}

#[test]
fn try_join_all_iter_lifetime() {
    // In futures-rs version 0.1, this function would fail to typecheck due to an overly
    // conservative type parameterization of `TryJoinAll`.
    fn sizes(bufs: Vec<&[u8]>) -> impl Future<Output = Result<Vec<usize>, ()>> {
        let iter = bufs.into_iter().map(|b| ok::<usize, ()>(b.len()));
        try_join_all(iter)
    }

    assert_done(sizes(vec![&[1, 2, 3], &[], &[0]]), Ok(vec![3_usize, 0, 1]));
}

#[test]
fn try_join_all_from_iter() {
    assert_done(
        vec![ok(1), ok(2)].into_iter().collect::<TryJoinAll<_>>(),
        Ok::<_, usize>(vec![1, 2]),
    )
}

#[test]
fn try_join_all_preserves_order() {
    for n in [1, 30, 31, 100] {
        // Futures that complete in reverse order.
        let futures = (0..n).map(|i| async move {
            for _ in i..n {
                ready(()).pending_once().await;
            }
            Ok::<_, ()>(i)
        });
        assert_done(try_join_all(futures), Ok((0..n).collect::<Vec<_>>()));
    }
}

#[test]
fn try_join_all_returns_first_error_immediately() {
    use futures::future::{Either, pending};

    for n in [2, 30, 31, 100] {
        // The first future never completes, the last one fails.
        let futures = (0..n).map(|i| {
            if i == 0 {
                Either::Left(pending::<Result<usize, usize>>())
            } else if i == n - 1 {
                Either::Right(err(i))
            } else {
                Either::Right(ok(i))
            }
        });
        assert_done(try_join_all(futures), Err(n - 1));
    }
}
