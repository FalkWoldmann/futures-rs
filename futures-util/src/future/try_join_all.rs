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
use super::slot_wakers::SlotWakers;
use super::{IntoFuture, TryFuture, TryMaybeDone, assert_future, join_all};
use crate::TryFutureExt;

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
    elems: Pin<Box<[TryMaybeDone<IntoFuture<F>>]>>,
    // Wakers which record the futures that were woken, so that only those are
    // polled. `None` if there are at most `join_all::SMALL` futures, which are
    // all polled whenever `TryJoinAll` is polled.
    #[cfg(target_has_atomic = "ptr")]
    wakers: Option<SlotWakers>,
}

impl<F> fmt::Debug for TryJoinAll<F>
where
    F: TryFuture + fmt::Debug,
    F::Ok: fmt::Debug,
    F::Error: fmt::Debug,
    F::Output: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TryJoinAll").field("elems", &self.elems).finish()
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
/// When there are many futures, only the ones that have been woken are polled,
/// like with [`FuturesUnordered`][crate::stream::FuturesUnordered].
///
/// This function is only available when the `std` or `alloc` feature of this
/// library is activated, and it is activated by default.
///
/// # See Also
///
/// [`FuturesOrdered`][crate::stream::FuturesOrdered] and its counterpart
/// [`FuturesUnordered`][crate::stream::FuturesUnordered] provide additional
/// functionality, such as:
///
///  * Adding new futures to the set even after it has been started.
///
///  * Receiving the output of each future as soon as it is available.
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
    let elems: Box<[_]> =
        iter.into_iter().map(|f| TryMaybeDone::Future(TryFutureExt::into_future(f))).collect();
    assert_future::<Result<Vec<<I::Item as TryFuture>::Ok>, <I::Item as TryFuture>::Error>, _>(
        TryJoinAll {
            #[cfg(target_has_atomic = "ptr")]
            wakers: join_all::slot_wakers(elems.len()),
            elems: elems.into(),
        },
    )
}

impl<F> Future for TryJoinAll<F>
where
    F: TryFuture,
{
    type Output = Result<Vec<F::Ok>, F::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        #[cfg(target_has_atomic = "ptr")]
        if let Some(wakers) = &mut this.wakers {
            let res = wakers.poll(
                this.elems.as_mut(),
                cx,
                |elem| matches!(elem, TryMaybeDone::Future(_)),
                |elem, cx| elem.try_poll(cx),
            );
            return match res {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => {
                    this.wakers = None;
                    Poll::Ready(Ok(take_outputs(&mut this.elems)))
                }
                Poll::Ready(Err(e)) => {
                    // Cancel all other futures.
                    this.wakers = None;
                    this.elems = Box::pin([]);
                    Poll::Ready(Err(e))
                }
            };
        }

        let mut state = FinalState::AllDone;

        for elem in join_all::iter_pin_mut(this.elems.as_mut()) {
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
            FinalState::AllDone => Poll::Ready(Ok(take_outputs(&mut this.elems))),
            FinalState::Error(e) => {
                this.elems = Box::pin([]);
                Poll::Ready(Err(e))
            }
        }
    }
}

fn take_outputs<F: TryFuture>(elems: &mut Pin<Box<[TryMaybeDone<IntoFuture<F>>]>>) -> Vec<F::Ok> {
    let mut elems = mem::replace(elems, Box::pin([]));
    join_all::iter_pin_mut(elems.as_mut()).map(|e| e.take_output().unwrap()).collect()
}

impl<F> FromIterator<F> for TryJoinAll<F>
where
    F: TryFuture,
{
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        try_join_all(iter)
    }
}
