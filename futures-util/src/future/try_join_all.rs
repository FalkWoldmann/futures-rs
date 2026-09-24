//! Definition of the `TryJoinAll` combinator, waiting for all of a list of
//! futures to finish with either success or error.

use alloc::{boxed::Box, vec::Vec};
use core::{
    fmt,
    future::Future,
    iter::FromIterator,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(target_has_atomic = "ptr")]
use futures_core::ready;

#[cfg(target_has_atomic = "ptr")]
use super::join_all::Collected;
use super::{IntoFuture, TryFuture, TryMaybeDone, assert_future, join_all};
use crate::TryFutureExt;
#[cfg(target_has_atomic = "ptr")]
use crate::stream::{FuturesUnordered, StreamExt, futures_ordered::OrderWrapper};

enum FinalState<E = ()> {
    Pending,
    AllDone,
    Error(E),
}

/// Future for the [`try_join_all`] function.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TryJoinAll<F>
where
    F: TryFuture,
{
    kind: TryJoinAllKind<F>,
}

enum TryJoinAllKind<F>
where
    F: TryFuture,
{
    Small {
        elems: Pin<Box<[TryMaybeDone<IntoFuture<F>>]>>,
    },
    #[cfg(target_has_atomic = "ptr")]
    Big {
        fut: FuturesUnordered<OrderWrapper<IntoFuture<F>>>,
        // The output of each future, stored at the future's index.
        outputs: Box<[Option<F::Ok>]>,
    },
}

impl<F> fmt::Debug for TryJoinAll<F>
where
    F: TryFuture + fmt::Debug,
    F::Ok: fmt::Debug,
    F::Error: fmt::Debug,
    F::Output: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            TryJoinAllKind::Small { ref elems } => {
                f.debug_struct("TryJoinAll").field("elems", elems).finish()
            }
            #[cfg(target_has_atomic = "ptr")]
            TryJoinAllKind::Big { ref fut, ref outputs } => {
                f.debug_struct("TryJoinAll").field("fut", fut).field("outputs", outputs).finish()
            }
        }
    }
}

/// Creates a future which represents either a collection of the results of the
/// futures given or an error.
///
/// The returned future will drive execution for all of its underlying futures,
/// collecting the results into a destination `Vec<T>` in the same order as they
/// were provided.
///
/// If any future returns an error then all other futures will be canceled and
/// an error will be returned immediately. If all futures complete successfully,
/// however, then the returned future will succeed with a `Vec` of all the
/// successful results.
///
/// This function is only available when the `std` or `alloc` feature of this
/// library is activated, and it is activated by default.
///
/// # See Also
///
/// `try_join_all` will switch to an implementation based on the more powerful
/// [`FuturesUnordered`][crate::stream::FuturesUnordered] for performance reasons
/// if the number of futures is large. You may want to look into using it or its
/// counterpart [`FuturesOrdered`][crate::stream::FuturesOrdered] directly.
///
/// Some examples for additional functionality provided by these are:
///
///  * Adding new futures to the set even after it has been started.
///
///  * Only polling the specific futures that have been woken. In cases where
///    you have a lot of futures this will result in much more efficient polling.
///
///
/// # Examples
///
/// ```
/// # futures::executor::block_on(async {
/// use futures::future::{self, try_join_all};
///
/// let futures = vec![
///     future::ok::<u32, u32>(1),
///     future::ok::<u32, u32>(2),
///     future::ok::<u32, u32>(3),
/// ];
///
/// assert_eq!(try_join_all(futures).await, Ok(vec![1, 2, 3]));
///
/// let futures = vec![
///     future::ok::<u32, u32>(1),
///     future::err::<u32, u32>(2),
///     future::ok::<u32, u32>(3),
/// ];
///
/// assert_eq!(try_join_all(futures).await, Err(2));
/// # });
/// ```
pub fn try_join_all<I>(iter: I) -> TryJoinAll<I::Item>
where
    I: IntoIterator,
    I::Item: TryFuture,
{
    let iter = iter.into_iter().map(TryFutureExt::into_future);

    #[cfg(not(target_has_atomic = "ptr"))]
    let kind = TryJoinAllKind::Small {
        elems: iter.map(TryMaybeDone::Future).collect::<Box<[_]>>().into(),
    };

    #[cfg(target_has_atomic = "ptr")]
    let kind = match join_all::collect_futures(iter, TryMaybeDone::Future) {
        Collected::Small(elems) => TryJoinAllKind::Small { elems: elems.into() },
        Collected::Big(fut) => TryJoinAllKind::Big { outputs: join_all::output_slots(&fut), fut },
    };

    assert_future::<Result<Vec<<I::Item as TryFuture>::Ok>, <I::Item as TryFuture>::Error>, _>(
        TryJoinAll { kind },
    )
}

impl<F> Future for TryJoinAll<F>
where
    F: TryFuture,
{
    type Output = Result<Vec<F::Ok>, F::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.kind {
            TryJoinAllKind::Small { elems } => {
                let mut state = FinalState::AllDone;

                for elem in join_all::iter_pin_mut(elems.as_mut()) {
                    match elem.try_poll(cx) {
                        Poll::Pending => state = FinalState::Pending,
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => {
                            state = FinalState::Error(e);
                            break;
                        }
                    }
                }

                match state {
                    FinalState::Pending => Poll::Pending,
                    FinalState::AllDone => {
                        let mut elems = mem::replace(elems, Box::pin([]));
                        let results = join_all::iter_pin_mut(elems.as_mut())
                            .map(|e| e.take_output().unwrap())
                            .collect();
                        Poll::Ready(Ok(results))
                    }
                    FinalState::Error(e) => {
                        let _ = mem::replace(elems, Box::pin([]));
                        Poll::Ready(Err(e))
                    }
                }
            }
            #[cfg(target_has_atomic = "ptr")]
            TryJoinAllKind::Big { fut, outputs } => poll_big(fut, outputs, cx),
        }
    }
}

// Not inlined to keep `TryJoinAll::poll` small for the `Small` case.
#[cfg(target_has_atomic = "ptr")]
#[inline(never)]
fn poll_big<F: TryFuture>(
    fut: &mut FuturesUnordered<OrderWrapper<IntoFuture<F>>>,
    outputs: &mut Box<[Option<F::Ok>]>,
    cx: &mut Context<'_>,
) -> Poll<Result<Vec<F::Ok>, F::Error>> {
    loop {
        match ready!(fut.poll_next_unpin(cx)) {
            Some(OrderWrapper { data: Ok(data), index }) => outputs[index as usize] = Some(data),
            Some(OrderWrapper { data: Err(e), .. }) => {
                // Cancel all other futures.
                fut.clear();
                *outputs = Box::new([]);
                return Poll::Ready(Err(e));
            }
            None => return Poll::Ready(Ok(join_all::take_outputs(outputs))),
        }
    }
}

impl<F> FromIterator<F> for TryJoinAll<F>
where
    F: TryFuture,
{
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        try_join_all(iter)
    }
}
