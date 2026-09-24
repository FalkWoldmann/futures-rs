//! Wakers for a fixed number of futures that record which futures were woken.
//!
//! [`JoinAll`](super::JoinAll) and [`TryJoinAll`](super::TryJoinAll) use this
//! when they are given many futures, so that a wake-up only causes the woken
//! future to be polled, like with
//! [`FuturesUnordered`](crate::stream::FuturesUnordered), but without
//! allocating a task for each future.
//!
//! Each future gets a slot, and the waker passed to the future points to its
//! slot. All slots live in a single allocation that is owned by a reference
//! counted [`Inner`], and every waker (other than the borrowed ones passed to
//! `poll`) holds a strong reference to it. Waking a slot pushes it onto an
//! intrusive MPSC queue (the 1024cores algorithm, like the ready to run queue
//! of `FuturesUnordered`) and wakes the task that polls the futures. The queue
//! never owns anything, so slots that are still enqueued when the futures are
//! dropped need no cleanup.

use alloc::{boxed::Box, sync::Arc};
use core::{
    cell::UnsafeCell,
    mem::{self, ManuallyDrop},
    pin::Pin,
    ptr::{self, NonNull},
    sync::atomic::{
        AtomicBool, AtomicPtr,
        Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use crate::task::{AtomicWaker, WakerRef};

/// The wakers of a fixed number of futures and the queue of woken futures.
pub(crate) struct SlotWakers {
    inner: Arc<Inner>,
    /// The index of the first future that has never been polled.
    unpolled: usize,
    /// The number of futures that have not completed.
    remaining: usize,
}

struct Inner {
    /// The waker of the task that polls the futures.
    parent: AtomicWaker,
    /// Whether the task that polls the futures may be waiting for a wake-up,
    /// in which case enqueueing a slot must wake `parent`.
    parked: AtomicBool,
    // Producer end of the ready queue.
    // Safety invariant: points to one of the `len + 1` slots at `slots`.
    head: AtomicPtr<Slot>,
    // Consumer end of the ready queue. It is only accessed by `dequeue`, whose
    // callers guarantee mutual exclusion.
    // Safety invariant: points to one of the `len + 1` slots at `slots`.
    tail: UnsafeCell<*const Slot>,
    // Safety invariant: `slots` is the pointer returned by `Box::into_raw` for
    // a `Box<[Slot]>` of length `len + 1`, which this `Inner` owns and frees
    // in its `Drop` impl. The last slot is the stub node of the ready queue.
    // No `&mut` reference to any slot is created while the `Inner` is alive.
    slots: NonNull<Slot>,
    len: usize,
}

// SAFETY: The raw pointers in `Inner` only point into the slot allocation that
// `Inner` owns. That allocation is only accessed through shared references to
// `Slot`, whose fields are atomics except `Slot::inner`, which is never written
// after `Inner` is constructed, and `tail`, which is only accessed by the single
// consumer (see `dequeue`). So sharing or sending `Inner` across threads cannot
// cause a data race. `AtomicWaker` is `Send + Sync`.
unsafe impl Send for Inner {}
// SAFETY: See the `Send` impl.
unsafe impl Sync for Inner {}

struct Slot {
    // Safety invariant: after `SlotWakers::new` returns, this is the pointer
    // returned by `Arc::into_raw` for the `Arc<Inner>` that owns this slot.
    // It is never written afterwards.
    inner: *const Inner,
    /// The next slot in the ready queue.
    next: AtomicPtr<Slot>,
    /// Whether waking this slot does nothing. This is the case while the slot
    /// is in the ready queue, before its future is polled for the first time
    /// (when no waker for it exists), and after its future has completed.
    queued: AtomicBool,
}

enum Dequeue {
    Data(*const Slot),
    Empty,
    Inconsistent,
}

impl SlotWakers {
    /// Creates the wakers for `len` futures.
    pub(crate) fn new(len: usize) -> Self {
        let slots: Box<[Slot]> = (0..=len)
            .map(|_| Slot {
                inner: ptr::null(),
                next: AtomicPtr::new(ptr::null_mut()),
                queued: AtomicBool::new(true),
            })
            .collect();
        // `Box::into_raw` for a non-empty slice never returns null.
        let slots = NonNull::new(Box::into_raw(slots).cast::<Slot>()).unwrap();
        // SAFETY: `slots` points to `len + 1` slots in one allocation, so the
        // stub at offset `len` is in bounds of that allocation.
        let stub = unsafe { slots.add(len) }.as_ptr();
        let inner = Arc::into_raw(Arc::new(Inner {
            parent: AtomicWaker::new(),
            parked: AtomicBool::new(false),
            head: AtomicPtr::new(stub),
            tail: UnsafeCell::new(stub),
            slots,
            len,
        }));
        for i in 0..=len {
            // SAFETY:
            // - `slots.add(i)` is in bounds because `i <= len` and the
            //   allocation holds `len + 1` slots.
            // - The write to `Slot::inner` is valid: the slot is initialized,
            //   aligned and part of the live allocation, and no reference to any
            //   slot exists yet (no waker was created), so the write cannot
            //   alias a reference.
            unsafe { (*slots.add(i).as_ptr()).inner = inner };
        }
        // SAFETY: `inner` was returned by `Arc::into_raw` above and ownership
        // of that strong reference is reclaimed exactly once, here.
        let inner = unsafe { Arc::from_raw(inner) };
        Self { inner, unpolled: 0, remaining: len }
    }

    /// Polls the futures in `elems` that may be able to make progress.
    ///
    /// `is_pending` tells whether an element has not completed yet, and
    /// `poll_elem` polls an element that has not completed, returning
    /// `Poll::Ready(Ok(()))` once it has. This returns `Poll::Ready(Ok(()))`
    /// once all elements have completed, or the first error returned by
    /// `poll_elem`.
    ///
    /// Like `FuturesUnordered`, this yields to the executor after polling as
    /// many futures as were incomplete, or after two futures woke themselves.
    ///
    /// # Panics
    ///
    /// Panics if `elems` does not have the length given to `new`.
    pub(crate) fn poll<T, E>(
        &mut self,
        mut elems: Pin<&mut [T]>,
        cx: &mut Context<'_>,
        is_pending: impl Fn(&T) -> bool,
        mut poll_elem: impl FnMut(Pin<&mut T>, &mut Context<'_>) -> Poll<Result<(), E>>,
    ) -> Poll<Result<(), E>> {
        assert_eq!(elems.len(), self.inner.len);
        if self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let limit = self.remaining;
        let mut polled = 0;
        let mut yielded = 0;
        let mut registered = false;

        loop {
            let index = if self.unpolled < self.inner.len {
                self.unpolled += 1;
                self.unpolled - 1
            } else {
                // SAFETY: `dequeue` requires that it is not called concurrently.
                // It is only called here, and `&mut self` guarantees that no
                // other call of `poll` on this `SlotWakers` is running. `inner`
                // is not shared with any other `SlotWakers`.
                match unsafe { self.inner.dequeue() } {
                    // The queue only ever holds slots of `inner` other than the
                    // stub, so this is an index into `elems`. It is computed
                    // from addresses, so a wrong pointer could only lead to a
                    // panicking index below, not to undefined behavior.
                    Dequeue::Data(slot) => {
                        (slot.addr() - self.inner.slots.as_ptr().addr()) / mem::size_of::<Slot>()
                    }
                    Dequeue::Empty => {
                        let first = !registered;
                        if first {
                            self.inner.parent.register(cx.waker());
                            registered = true;
                            // Announce that we may wait for a wake-up.
                            self.inner.parked.store(true, SeqCst);
                        }
                        // Check whether a slot was enqueued after `dequeue`
                        // looked at the queue. The store of `parked` above, this
                        // load, the swap of `head` in `enqueue` and the load of
                        // `parked` after it in `wake_by_ref` are all `SeqCst`. In
                        // their single total order, either the swap comes before
                        // this load, which then sees it, or the store of
                        // `parked` comes before the load of `parked` by the
                        // waker, which then wakes us (unless another waker
                        // already cleared `parked` and woke us).
                        if ptr::eq(self.inner.head.load(SeqCst), self.inner.stub()) {
                            return Poll::Pending;
                        }
                        if first {
                            continue;
                        }
                        // A slot is being enqueued, but the link to it is not
                        // visible yet, and its waker may not wake us.
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Dequeue::Inconsistent => {
                        // A producer is in the middle of enqueueing a slot.
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
            };

            // A slot may be dequeued after its future has completed, if it
            // was woken during or after its last poll.
            if !is_pending(&elems[index]) {
                continue;
            }

            let slot = self.inner.slot(index);
            // Clear the flag before polling, so that a wake-up during `poll`
            // enqueues the slot. Acquire synchronizes with wakers that found
            // the flag already set.
            let prev = slot.queued.swap(false, AcqRel);
            debug_assert!(prev);

            let waker = slot.waker_ref();
            // SAFETY: Contract of `Pin::map_unchecked_mut`: the returned
            // reference must not be moved out of while pinned. It is a
            // reference to an element of the pinned slice, and elements of a
            // pinned slice are never moved (slices cannot be resized), so the
            // pinning guarantee of `elems` extends to its elements.
            let elem = unsafe { elems.as_mut().map_unchecked_mut(|elems| &mut elems[index]) };
            let res = poll_elem(elem, &mut Context::from_waker(&waker));
            polled += 1;

            match res {
                Poll::Ready(Ok(())) => {
                    // Later wake-ups have no effect.
                    slot.queued.store(true, Relaxed);
                    self.remaining -= 1;
                    if self.remaining == 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {
                    // If the future was woken while it was polled, assume it
                    // wanted to yield.
                    yielded += slot.queued.load(Relaxed) as usize;
                    if yielded >= 2 {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
            }

            // Yield after polling each incomplete future once on average, to
            // avoid starving other tasks.
            if polled == limit {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }
}

impl Drop for SlotWakers {
    fn drop(&mut self) {
        // Wakers can outlive `self`. Drop the parent waker now so that they
        // don't wake a task that no longer polls the futures.
        drop(self.inner.parent.take());
    }
}

impl Inner {
    /// Returns the slot of the future at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.len`.
    #[inline]
    fn slot(&self, index: usize) -> &Slot {
        assert!(index < self.len);
        // SAFETY:
        // - `add`: `index < len` (checked above), and the allocation holds
        //   `len + 1` slots, so the result is in bounds of it.
        // - Creating `&Slot`: the pointee is an initialized, aligned `Slot` in
        //   the slot allocation, which lives as long as `self` (invariant of
        //   `slots`). No `&mut Slot` exists while `self` is alive (invariant of
        //   `slots`), and mutation through `&Slot` only happens through atomics.
        unsafe { &*self.slots.as_ptr().add(index) }
    }

    #[inline]
    fn stub(&self) -> *const Slot {
        // SAFETY: The allocation holds `len + 1` slots, so offset `len` is in
        // bounds of it.
        unsafe { self.slots.as_ptr().add(self.len) }
    }

    /// The enqueue function from the 1024cores intrusive MPSC queue algorithm.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `slot` is one of the slots of `self`, which
    /// maintains the invariants of `head` and `tail`.
    ///
    /// The queue is only correct if a slot is never enqueued while it is still
    /// in the queue. Callers ensure this by only enqueueing a slot after
    /// changing its `queued` flag from `false` to `true`, and `dequeue` only
    /// enqueues the stub after it has been removed from the queue. Violating
    /// this can cause wrong results, but no undefined behavior.
    #[inline]
    unsafe fn enqueue(&self, slot: &Slot) {
        slot.next.store(ptr::null_mut(), Relaxed);
        let slot = ptr::from_ref(slot).cast_mut();
        // `SeqCst` for the handshake with `parked` (see `SlotWakers::poll`).
        let prev = self.head.swap(slot, SeqCst);
        // SAFETY: `prev` was stored in `head`, which only ever holds pointers to
        // slots of `self` (invariant of `head`, maintained because the caller
        // only passes slots of `self`), and those are alive while `self` is.
        // Only the atomic `next` field is accessed.
        unsafe { (*prev).next.store(slot, Release) };
    }

    /// The dequeue function from the 1024cores intrusive MPSC queue algorithm.
    ///
    /// # Safety
    ///
    /// The caller must ensure that this is not called concurrently with
    /// itself, as it accesses `tail` without synchronization.
    #[inline]
    unsafe fn dequeue(&self) -> Dequeue {
        // SAFETY (for the whole function):
        // - Accesses to `*self.tail.get()`: the caller guarantees that no other
        //   `dequeue` runs concurrently, and nothing else accesses `tail`.
        // - Dereferences of `tail` and `next`: they were loaded from `tail`,
        //   `head` or a slot's `next`, which only hold null (checked before
        //   dereferencing) or pointers to slots of `self`, which are alive while
        //   `self` is. Only atomic fields are accessed through them.
        // - `enqueue(&*self.stub())`: the stub is a slot of `self`, and creating
        //   `&Slot` for it is valid for the same reasons as in `Inner::slot`.
        unsafe {
            let mut tail = *self.tail.get();
            let mut next = (*tail).next.load(Acquire);

            if tail == self.stub() {
                if next.is_null() {
                    return Dequeue::Empty;
                }

                *self.tail.get() = next;
                tail = next;
                next = (*next).next.load(Acquire);
            }

            if !next.is_null() {
                *self.tail.get() = next;
                debug_assert!(tail != self.stub());
                return Dequeue::Data(tail);
            }

            if !ptr::eq(self.head.load(Acquire), tail) {
                return Dequeue::Inconsistent;
            }

            self.enqueue(&*self.stub());

            next = (*tail).next.load(Acquire);

            if !next.is_null() {
                *self.tail.get() = next;
                return Dequeue::Data(tail);
            }

            Dequeue::Inconsistent
        }
    }
}

impl Slot {
    /// Returns a waker for the future of this slot that borrows the slot
    /// instead of owning a reference count.
    #[inline]
    fn waker_ref(&self) -> WakerRef<'_> {
        let data = ptr::from_ref(self).cast::<()>();
        // SAFETY: Contract of `Waker::new`: the vtable functions must uphold the
        // `RawWakerVTable` contract for `data`, which points to `self`.
        // - Every `&Slot` is obtained from `Inner::slot` (or a waker), so the
        //   `Inner` owning `self` has a strong reference count of at least 1
        //   while `self` is borrowed, and `self` is alive. The returned
        //   `WakerRef` borrows `self`, so this holds while the waker is used.
        // - The `ManuallyDrop` in `WakerRef` ensures that `drop_waker` is never
        //   called for this waker, which does not own a reference count. `wake`
        //   (which consumes a waker) can only be called on clones, which own one
        //   (see `clone_waker`). See `VTABLE` for the other vtable functions.
        WakerRef::new_unowned(ManuallyDrop::new(unsafe { Waker::new(data, &VTABLE) }))
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let slots = ptr::slice_from_raw_parts_mut(self.slots.as_ptr(), self.len + 1);
        // SAFETY: Contract of `Box::from_raw`: the pointer must come from
        // `Box::into_raw` for the same type and must not be used afterwards.
        // By the invariant of `slots`, it was returned by `Box::into_raw` for a
        // `Box<[Slot]>` of length `len + 1`, and it is freed only here, as this
        // `Inner` is being dropped. Every waker holds a strong reference to this
        // `Inner` and borrowed wakers borrow a `SlotWakers` that does, so no
        // waker and no reference to a slot exists any more.
        drop(unsafe { Box::from_raw(slots) });
    }
}

/// The vtable of slot wakers. The data pointer of a slot waker points to a
/// `Slot`, and every slot waker except those returned by `waker_ref` owns a
/// strong reference count of the slot's `Inner`, which keeps the slot alive.
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

/// # Safety
///
/// The caller must ensure that `data` points to a `Slot` of an `Inner` that
/// is alive for the duration of the call.
unsafe fn slot<'a>(data: *const ()) -> &'a Slot {
    // SAFETY: The caller guarantees that `data` points to a slot of a live
    // `Inner`; slots are initialized and aligned, and no `&mut Slot` exists
    // while the `Inner` is alive (invariant of `Inner::slots`).
    unsafe { &*data.cast::<Slot>() }
}

unsafe fn clone_waker(data: *const ()) -> RawWaker {
    // SAFETY: `data` is the data pointer of a slot waker, which is alive
    // while the waker is, and the `RawWakerVTable` contract guarantees that
    // the waker is alive during this call.
    let inner = unsafe { slot(data) }.inner;
    // SAFETY: Contract of `Arc::increment_strong_count`: `inner` was returned
    // by `Arc::into_raw` (invariant of `Slot::inner`) and the strong count is
    // at least 1 for the duration of the call, because the waker being cloned
    // owns a strong reference or borrows a `SlotWakers` that does.
    unsafe { Arc::increment_strong_count(inner) };
    // The new waker owns the strong reference acquired above.
    RawWaker::new(data, &VTABLE)
}

unsafe fn wake(data: *const ()) {
    // SAFETY: `data` is the data pointer of an owned slot waker, which is
    // alive until the end of this call (see `drop_waker`).
    unsafe { wake_by_ref(data) };
    // SAFETY: `wake` consumes the waker, so its strong reference is released.
    unsafe { drop_waker(data) };
}

unsafe fn wake_by_ref(data: *const ()) {
    // SAFETY: `data` is the data pointer of a slot waker that is alive during
    // this call, so its slot and `Inner` are too.
    let slot = unsafe { slot(data) };
    if !slot.queued.swap(true, AcqRel) {
        // SAFETY: `slot.inner` points to the `Inner` that owns `slot`
        // (invariant of `Slot::inner`), which is alive as shown above.
        let inner = unsafe { &*slot.inner };
        // SAFETY: `slot` is a slot of `inner`. The flag changed from `false`
        // to `true`, so the slot is not in the queue.
        unsafe { inner.enqueue(slot) };
        // Only wake the parent task if it may be waiting for a wake-up. See
        // `SlotWakers::poll` for why this does not miss wake-ups.
        if inner.parked.load(SeqCst) && inner.parked.swap(false, AcqRel) {
            inner.parent.wake();
        }
    }
}

unsafe fn drop_waker(data: *const ()) {
    // SAFETY: `data` is the data pointer of an owned slot waker that is alive
    // during this call.
    let inner = unsafe { slot(data) }.inner;
    // SAFETY: Contract of `Arc::decrement_strong_count`: `inner` was returned
    // by `Arc::into_raw` (invariant of `Slot::inner`), and the waker being
    // dropped owns a strong reference, which is released here. The slot is
    // not accessed after this point, as it may have been freed.
    unsafe { Arc::decrement_strong_count(inner) };
}
