//! Definition of the `JoinAll` combinator, waiting for all of a list of futures
//! to finish.

use alloc::{boxed::Box, vec::Vec};
#[cfg(target_has_atomic = "ptr")]
use core::convert::Infallible;
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
use super::{MaybeDone, assert_future};

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
    elems: Pin<Box<[MaybeDone<F>]>>,
    // Wakers which record the futures that were woken, so that only those are
    // polled. `None` if there are at most `SMALL` futures, which are all polled
    // whenever `JoinAll` is polled.
    #[cfg(target_has_atomic = "ptr")]
    wakers: Option<SlotWakers>,
}

/// The largest number of futures that [`JoinAll`] and
/// [`TryJoinAll`](super::TryJoinAll) poll all at once.
#[cfg(target_has_atomic = "ptr")]
pub(crate) const SMALL: usize = 30;

/// Returns the wakers for `len` futures if there are more than `SMALL`.
#[cfg(target_has_atomic = "ptr")]
pub(crate) fn slot_wakers(len: usize) -> Option<SlotWakers> {
    (len > SMALL).then(|| SlotWakers::new(len))
}

impl<F> fmt::Debug for JoinAll<F>
where
    F: Future + fmt::Debug,
    F::Output: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinAll").field("elems", &self.elems).finish()
    }
}

/// Creates a future which represents a collection of the outputs of the futures
/// given.
///
/// The returned future will drive execution for all of its underlying futures,
/// collecting the results into a destination `Vec<T>` in the same order as they
/// were provided.
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
    let elems: Box<[_]> = iter.into_iter().map(MaybeDone::Future).collect();
    assert_future::<Vec<<I::Item as Future>::Output>, _>(JoinAll {
        #[cfg(target_has_atomic = "ptr")]
        wakers: slot_wakers(elems.len()),
        elems: elems.into(),
    })
}

impl<F> Future for JoinAll<F>
where
    F: Future,
{
    type Output = Vec<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        #[cfg(target_has_atomic = "ptr")]
        if let Some(wakers) = &mut this.wakers {
            let res = wakers.poll(
                this.elems.as_mut(),
                cx,
                |elem| matches!(elem, MaybeDone::Future(_)),
                |elem, cx| elem.poll(cx).map(Ok::<_, Infallible>),
            );
            return match res {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => {
                    this.wakers = None;
                    Poll::Ready(take_outputs(&mut this.elems))
                }
            };
        }

        let mut all_done = true;

        for elem in iter_pin_mut(this.elems.as_mut()) {
            if elem.poll(cx).is_pending() {
                all_done = false;
            }
        }

        if all_done { Poll::Ready(take_outputs(&mut this.elems)) } else { Poll::Pending }
    }
}

fn take_outputs<F: Future>(elems: &mut Pin<Box<[MaybeDone<F>]>>) -> Vec<F::Output> {
    let mut elems = mem::replace(elems, Box::pin([]));
    iter_pin_mut(elems.as_mut()).map(|e| e.take_output().unwrap()).collect()
}

impl<F: Future> FromIterator<F> for JoinAll<F> {
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        join_all(iter)
    }
}
