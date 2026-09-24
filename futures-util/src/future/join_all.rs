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
use super::wake_groups::WakeGroups;
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
        groups: WakeGroups<MaybeDone<F>>,
    },
}

/// Collects the futures yielded by `iter`, in a boxed slice if there are at
/// most `SMALL` of them, and in `WakeGroups` otherwise.
#[cfg(target_has_atomic = "ptr")]
pub(crate) fn collect<I: Iterator>(mut iter: I) -> Result<Box<[I::Item]>, WakeGroups<I::Item>> {
    match iter.size_hint() {
        (_, Some(max)) if max <= SMALL => Ok(iter.collect()),
        (min, _) if min > SMALL => Err(WakeGroups::new(iter)),
        _ => {
            // The size hint is inconclusive, so buffer up to `SMALL + 1`
            // futures to find out whether there are more than `SMALL`.
            let head: Vec<_> = iter.by_ref().take(SMALL + 1).collect();
            if head.len() <= SMALL {
                Ok(head.into_boxed_slice())
            } else {
                Err(WakeGroups::new(head.into_iter().chain(iter)))
            }
        }
    }
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
            JoinAllKind::Big { ref groups } => {
                f.debug_struct("JoinAll").field("elems", groups).finish()
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
/// If there are many futures, a wake-up only causes the futures near the woken
/// one to be polled, like with [`FuturesUnordered`][crate::stream::FuturesUnordered].
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
    let iter = iter.into_iter();

    #[cfg(not(target_has_atomic = "ptr"))]
    {
        let kind =
            JoinAllKind::Small { elems: iter.map(MaybeDone::Future).collect::<Box<[_]>>().into() };

        assert_future::<Vec<<I::Item as Future>::Output>, _>(JoinAll { kind })
    }

    #[cfg(target_has_atomic = "ptr")]
    {
        let kind = match collect(iter.map(MaybeDone::Future)) {
            Ok(elems) => JoinAllKind::Small { elems: elems.into() },
            Err(groups) => JoinAllKind::Big { groups },
        };

        assert_future::<Vec<<I::Item as Future>::Output>, _>(JoinAll { kind })
    }
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
            JoinAllKind::Big { groups } => {
                let res = groups.poll(
                    cx,
                    |elem| matches!(elem, MaybeDone::Future(_)),
                    |elem, cx| elem.poll(cx).map(Ok::<_, Infallible>),
                );
                match res {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => match e {},
                }
                let result = groups.iter_pin_mut().map(|e| e.take_output().unwrap()).collect();
                self.kind = JoinAllKind::Small { elems: Box::pin([]) };
                Poll::Ready(result)
            }
        }
    }
}

impl<F: Future> FromIterator<F> for JoinAll<F> {
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        join_all(iter)
    }
}
