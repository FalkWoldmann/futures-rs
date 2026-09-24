//! Definition of the `JoinAll` combinator, waiting for all of a list of futures
//! to finish.

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

use super::{MaybeDone, assert_future};
#[cfg(target_has_atomic = "ptr")]
use crate::stream::{FuturesUnordered, StreamExt, futures_ordered::OrderWrapper};

pub(crate) fn iter_pin_mut<T>(slice: Pin<&mut [T]>) -> impl Iterator<Item = Pin<&mut T>> {
    // Safety: `std` _could_ make this unsound if it were to decide Pin's
    // invariants aren't required to transmit through slices. Otherwise this has
    // the same safety as a normal field pin projection.
    unsafe { slice.get_unchecked_mut() }.iter_mut().map(|t| unsafe { Pin::new_unchecked(t) })
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
/// Future for the [`join_all`] function.
pub struct JoinAll<F>
where
    F: Future,
{
    kind: JoinAllKind<F>,
}

#[cfg(target_has_atomic = "ptr")]
pub(crate) const SMALL: usize = 30;

enum JoinAllKind<F>
where
    F: Future,
{
    Small {
        elems: Pin<Box<[MaybeDone<F>]>>,
    },
    #[cfg(target_has_atomic = "ptr")]
    Big {
        fut: FuturesUnordered<OrderWrapper<F>>,
        // The output of each future, stored at the future's index.
        outputs: Box<[Option<F::Output>]>,
    },
}

/// The futures of a [`JoinAll`] or [`TryJoinAll`](super::TryJoinAll), collected
/// into the representation that is appropriate for their number.
#[cfg(target_has_atomic = "ptr")]
pub(crate) enum Collected<W, F> {
    Small(Box<[W]>),
    Big(FuturesUnordered<OrderWrapper<F>>),
}

/// Collects the futures yielded by `iter`, wrapping each of them with `wrap` if
/// there are at most `SMALL` of them and tagging each of them with its index
/// otherwise.
#[cfg(target_has_atomic = "ptr")]
pub(crate) fn collect_futures<I, W>(
    mut iter: I,
    wrap: impl FnMut(I::Item) -> W,
) -> Collected<W, I::Item>
where
    I: Iterator,
{
    fn index_all<F>(iter: impl Iterator<Item = F>) -> FuturesUnordered<OrderWrapper<F>> {
        iter.enumerate().map(|(i, data)| OrderWrapper { data, index: i as i64 }).collect()
    }

    match iter.size_hint() {
        (_, Some(max)) if max <= SMALL => Collected::Small(iter.map(wrap).collect()),
        (min, _) if min > SMALL => Collected::Big(index_all(iter)),
        _ => {
            // The size hint is inconclusive, so buffer up to `SMALL + 1`
            // futures to find out whether there are more than `SMALL`.
            let head: Vec<_> = iter.by_ref().take(SMALL + 1).collect();
            if head.len() <= SMALL {
                Collected::Small(head.into_iter().map(wrap).collect())
            } else {
                Collected::Big(index_all(head.into_iter().chain(iter)))
            }
        }
    }
}

/// Allocates a slot for the output of each of the given futures.
#[cfg(target_has_atomic = "ptr")]
pub(crate) fn output_slots<F, T>(fut: &FuturesUnordered<F>) -> Box<[Option<T>]> {
    core::iter::repeat_with(|| None).take(fut.len()).collect()
}

/// Unwraps the outputs of all futures once they have completed.
#[cfg(target_has_atomic = "ptr")]
pub(crate) fn take_outputs<T>(outputs: &mut Box<[Option<T>]>) -> Vec<T> {
    mem::take(outputs).into_vec().into_iter().map(|output| output.unwrap()).collect()
}

impl<F> fmt::Debug for JoinAll<F>
where
    F: Future + fmt::Debug,
    F::Output: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            JoinAllKind::Small { ref elems } => {
                f.debug_struct("JoinAll").field("elems", elems).finish()
            }
            #[cfg(target_has_atomic = "ptr")]
            JoinAllKind::Big { ref fut, ref outputs } => {
                f.debug_struct("JoinAll").field("fut", fut).field("outputs", outputs).finish()
            }
        }
    }
}

/// Creates a future which represents a collection of the outputs of the futures
/// given.
///
/// The returned future will drive execution for all of its underlying futures,
/// collecting the results into a destination `Vec<T>` in the same order as they
/// were provided.
///
/// This function is only available when the `std` or `alloc` feature of this
/// library is activated, and it is activated by default.
///
/// # See Also
///
/// `join_all` will switch to an implementation based on the more powerful
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
/// # Examples
///
/// ```
/// # futures::executor::block_on(async {
/// use futures::future::join_all;
///
/// async fn foo(i: u32) -> u32 { i }
///
/// let futures = vec![foo(1), foo(2), foo(3)];
///
/// assert_eq!(join_all(futures).await, [1, 2, 3]);
/// # });
/// ```
pub fn join_all<I>(iter: I) -> JoinAll<I::Item>
where
    I: IntoIterator,
    I::Item: Future,
{
    let iter = iter.into_iter();

    #[cfg(not(target_has_atomic = "ptr"))]
    let kind =
        JoinAllKind::Small { elems: iter.map(MaybeDone::Future).collect::<Box<[_]>>().into() };

    #[cfg(target_has_atomic = "ptr")]
    let kind = match collect_futures(iter, MaybeDone::Future) {
        Collected::Small(elems) => JoinAllKind::Small { elems: elems.into() },
        Collected::Big(fut) => JoinAllKind::Big { outputs: output_slots(&fut), fut },
    };

    assert_future::<Vec<<I::Item as Future>::Output>, _>(JoinAll { kind })
}

impl<F> Future for JoinAll<F>
where
    F: Future,
{
    type Output = Vec<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.kind {
            JoinAllKind::Small { elems } => {
                let mut all_done = true;

                for elem in iter_pin_mut(elems.as_mut()) {
                    if elem.poll(cx).is_pending() {
                        all_done = false;
                    }
                }

                if all_done {
                    let mut elems = mem::replace(elems, Box::pin([]));
                    let result =
                        iter_pin_mut(elems.as_mut()).map(|e| e.take_output().unwrap()).collect();
                    Poll::Ready(result)
                } else {
                    Poll::Pending
                }
            }
            #[cfg(target_has_atomic = "ptr")]
            JoinAllKind::Big { fut, outputs } => poll_big(fut, outputs, cx),
        }
    }
}

// Not inlined to keep `JoinAll::poll` small for the `Small` case.
#[cfg(target_has_atomic = "ptr")]
#[inline(never)]
fn poll_big<F: Future>(
    fut: &mut FuturesUnordered<OrderWrapper<F>>,
    outputs: &mut Box<[Option<F::Output>]>,
    cx: &mut Context<'_>,
) -> Poll<Vec<F::Output>> {
    loop {
        match ready!(fut.poll_next_unpin(cx)) {
            Some(OrderWrapper { data, index }) => outputs[index as usize] = Some(data),
            None => return Poll::Ready(take_outputs(outputs)),
        }
    }
}

impl<F: Future> FromIterator<F> for JoinAll<F> {
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        join_all(iter)
    }
}
