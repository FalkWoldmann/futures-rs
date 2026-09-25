//! Polling of many futures such that only the ones that were woken are polled.
//!
//! [`JoinAll`](super::JoinAll) and [`TryJoinAll`](super::TryJoinAll) use this
//! when they are given many futures. The futures are split into groups of
//! `GROUP_SIZE`, and each group gets a waker, so that a wake-up only causes the
//! futures in the woken group to be polled. The futures are stored in one
//! boxed slice, and each group's waker needs one allocation, instead of a task
//! per future as with [`FuturesUnordered`](crate::stream::FuturesUnordered).
//!
//! The woken groups are recorded in a [`ReadySet`], a tree of atomic bit sets
//! that wakers can update without locks and without allocating.

use alloc::{boxed::Box, sync::Arc, task::Wake, vec::Vec};
use core::{
    fmt,
    pin::Pin,
    sync::atomic::{
        AtomicBool, AtomicUsize,
        Ordering::{AcqRel, Relaxed, SeqCst},
    },
    task::{Context, Poll, Waker},
};

use super::join_all::iter_pin_mut;
use crate::task::AtomicWaker;

/// The number of futures that share a waker.
const GROUP_SIZE: usize = 8;

const BITS: usize = usize::BITS as usize;

/// Futures split into groups, each with its own waker.
pub(crate) struct WakeGroups<T> {
    /// The elements; group `i` is `elems[i * GROUP_SIZE..][..GROUP_SIZE]`.
    elems: Pin<Box<[T]>>,
    /// One waker per group.
    wakers: Box<[Waker]>,
    shared: Arc<Shared>,
    /// The groups to poll, and the position of the next one in it.
    queue: Vec<usize>,
    next: usize,
    /// The number of elements that have not completed.
    remaining: usize,
}

struct Shared {
    /// The waker of the task that polls the futures.
    parent: AtomicWaker,
    /// Whether the task that polls the futures may be waiting for a wake-up,
    /// in which case waking a group must wake `parent`.
    parked: AtomicBool,
    /// The groups that were woken.
    ready: ReadySet,
}

struct GroupWaker {
    shared: Arc<Shared>,
    group: usize,
}

impl Wake for GroupWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let shared = &self.shared;
        // Only wake the parent task if it may be waiting for a wake-up. See
        // `WakeGroups::poll` for why this does not miss wake-ups.
        if shared.ready.insert(self.group)
            && shared.parked.load(SeqCst)
            && shared.parked.swap(false, AcqRel)
        {
            shared.parent.wake();
        }
    }
}

impl<T> WakeGroups<T> {
    /// Splits the elements yielded by `iter` into groups.
    pub(crate) fn new(iter: impl Iterator<Item = T>) -> Self {
        let elems: Box<[T]> = iter.collect();
        let remaining = elems.len();
        let groups = (remaining + GROUP_SIZE - 1) / GROUP_SIZE;

        let shared = Arc::new(Shared {
            parent: AtomicWaker::new(),
            parked: AtomicBool::new(false),
            ready: ReadySet::new(groups),
        });
        let wakers = (0..groups)
            .map(|group| Waker::from(Arc::new(GroupWaker { shared: shared.clone(), group })))
            .collect();
        // Every group is polled once before waiting for wake-ups.
        let queue = (0..groups).collect();

        Self { elems: elems.into(), wakers, shared, queue, next: 0, remaining }
    }

    /// Returns an iterator over the elements.
    pub(crate) fn iter_pin_mut(&mut self) -> impl Iterator<Item = Pin<&mut T>> {
        iter_pin_mut(self.elems.as_mut(), ..)
    }

    /// Polls the elements in the groups that may be able to make progress.
    ///
    /// `is_pending` tells whether an element has not completed yet, and
    /// `poll_elem` polls an element that has not completed, returning
    /// `Poll::Ready(Ok(()))` once it has. This returns `Poll::Ready(Ok(()))`
    /// once all elements have completed, or the first error returned by
    /// `poll_elem`.
    ///
    /// Like `FuturesUnordered`, this yields to the executor after polling as
    /// many groups as there are, or after two groups woke themselves.
    pub(crate) fn poll<E>(
        &mut self,
        cx: &mut Context<'_>,
        is_pending: impl Fn(&T) -> bool,
        mut poll_elem: impl FnMut(Pin<&mut T>, &mut Context<'_>) -> Poll<Result<(), E>>,
    ) -> Poll<Result<(), E>> {
        if self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let limit = self.wakers.len();
        let mut polled = 0;
        let mut yielded = 0;
        let mut registered = false;

        loop {
            if self.next == self.queue.len() {
                self.queue.clear();
                self.next = 0;
                self.shared.ready.take_all(&mut self.queue);
                if self.queue.is_empty() {
                    if !registered {
                        self.shared.parent.register(cx.waker());
                        registered = true;
                        // Announce that we may wait for a wake-up.
                        self.shared.parked.store(true, SeqCst);
                    }
                    // Check whether a group was woken after `take_all`. The
                    // store of `parked` above and this load are `SeqCst`, like
                    // the update of the root word in `ReadySet::insert` and the
                    // load of `parked` after it in `wake_by_ref`. In their
                    // single total order, either the update comes before this
                    // load, which then sees it, or the store of `parked` comes
                    // before the load of `parked` by the waker, which then
                    // wakes us (unless another waker already cleared `parked`
                    // and woke us). A waker that doesn't update the root word
                    // set a bit that `take_all` above has taken, or that is
                    // reachable from a root bit set by another waker.
                    if self.shared.ready.is_empty() {
                        return Poll::Pending;
                    }
                    continue;
                }
            }

            let group = self.queue[self.next];
            self.next += 1;

            let mut group_cx = Context::from_waker(&self.wakers[group]);
            let start = group * GROUP_SIZE;
            let end = (start + GROUP_SIZE).min(self.elems.len());
            for elem in iter_pin_mut(self.elems.as_mut(), start..end) {
                if !is_pending(elem.as_ref().get_ref()) {
                    continue;
                }
                match poll_elem(elem, &mut group_cx) {
                    Poll::Ready(Ok(())) => self.remaining -= 1,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {}
                }
            }
            if self.remaining == 0 {
                return Poll::Ready(Ok(()));
            }
            polled += 1;

            // If the group was woken while it was polled, assume that one of
            // its futures wanted to yield.
            if self.shared.ready.contains(group) {
                yielded += 1;
            }
            // Yield after two groups yielded, or after polling each group once
            // on average, to avoid starving other tasks.
            if yielded >= 2 || polled == limit {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }
}

impl<T> Drop for WakeGroups<T> {
    fn drop(&mut self) {
        // Wakers can outlive `self`. Drop the parent waker now so that they
        // don't wake a task that no longer polls the futures.
        drop(self.shared.parent.take());
    }
}

/// A set of indices that any thread can insert into, and that one thread takes
/// all indices from at once.
///
/// It is a tree of bit sets: `levels[0]` has a bit for each index, each further
/// level has a bit for each word of the level below it, and the last level (the
/// root) is a single word. A bit is set in a word only if the word was zero
/// before, or if the thread that set its first bit is still going to set the
/// bit for the word in the level above. So every set bit can be reached from
/// the root, or will be once concurrent insertions finish.
struct ReadySet {
    levels: Box<[Box<[AtomicUsize]>]>,
}

impl ReadySet {
    fn new(len: usize) -> Self {
        let mut levels = Vec::new();
        let mut bits = len;
        loop {
            let words = ((bits + BITS - 1) / BITS).max(1);
            levels.push((0..words).map(|_| AtomicUsize::new(0)).collect());
            if words == 1 {
                break;
            }
            bits = words;
        }
        Self { levels: levels.into() }
    }

    /// Inserts `index`. Returns whether this changed the root word from zero,
    /// which is when the caller must make sure that the set is taken from.
    fn insert(&self, mut index: usize) -> bool {
        for level in self.levels.iter() {
            // `SeqCst` for the handshake in `WakeGroups::poll` (when this is the
            // root word), and to make the writes of the waking thread visible
            // to the thread that takes the index.
            if level[index / BITS].fetch_or(1 << (index % BITS), SeqCst) != 0 {
                // Another thread set a bit in this word first, and is
                // responsible for the levels above.
                return false;
            }
            index /= BITS;
        }
        true
    }

    /// Returns whether `index` is in the set.
    fn contains(&self, index: usize) -> bool {
        self.levels[0][index / BITS].load(Relaxed) & (1 << (index % BITS)) != 0
    }

    /// Returns whether the root word is zero.
    fn is_empty(&self) -> bool {
        self.levels[self.levels.len() - 1][0].load(SeqCst) == 0
    }

    /// Removes all indices from the set and appends them to `out`.
    fn take_all(&self, out: &mut Vec<usize>) {
        self.take_word(self.levels.len() - 1, 0, out);
    }

    fn take_word(&self, level: usize, word: usize, out: &mut Vec<usize>) {
        // A word is always taken before the words below it, so that bits set
        // in them after this point are reachable from a bit set above.
        let mut bits = self.levels[level][word].swap(0, AcqRel);
        while bits != 0 {
            let index = word * BITS + bits.trailing_zeros() as usize;
            bits &= bits - 1;
            if level == 0 {
                out.push(index);
            } else {
                self.take_word(level - 1, index, out);
            }
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for WakeGroups<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.elems.iter()).finish()
    }
}
