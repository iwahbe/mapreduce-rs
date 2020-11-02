#![warn(missing_docs)]

//! Implements a process based map-reduce library, made to link with a C header
//! `mapreduce.h`. This discourages an idiomatic rust approach. Because
//! [`Getter`](Getter) and [`MR_Emit`](MR_Emit) do not take any arguments for
//! managing state, this necessitates a mutable global [`EMITTED`](EMITTED)
//! object.

#[cfg(test)]
mod tests {

    #[allow(unused_imports)]
    use super::*;

    extern "C" fn dummy_partition(partition: usize, _: c_int) -> c_ulong {
        partition as c_ulong
    }

    #[test]
    fn test_emit_single_thread() {
        let emitter = GlobalEmit::new(dummy_partition, &|x| *x);
        emitter.setup(dummy_partition, 5);

        emitter.emit(&1, 2);
        emitter.emit(&1, 3);
        emitter.emit(&1, 1);

        assert_eq!(emitter.get(&1, 1), Some(1));
        assert_eq!(emitter.get(&1, 1), Some(2));
        assert_eq!(emitter.get(&1, 1), Some(3));
    }

    #[test]
    fn test_emit_multi_threaded() {
        use std::sync::Arc;
        use std::thread;
        let clos: &'static (dyn Send + Fn(&usize) -> usize) = &|x| *x;
        let emit = GlobalEmit::new(dummy_partition, clos);

        emit.setup(dummy_partition, 3);
        let emitter = Arc::new(ThreadSafe(emit));
        let mut v = Vec::new();

        for i in (1..15).rev() {
            let emit = Arc::clone(&emitter);
            let clos = move || {
                emit.0.emit(&2, i * 2);
                emit.0.emit(&1, 2);
            };
            v.push(thread::spawn(clos));
        }

        // Must join for consistant tests
        v.into_iter().for_each(|t| t.join().unwrap());

        let emit = &emitter.0;
        for i in 1..15 {
            assert_eq!(emit.get(&1, 1), Some(2));
            assert_eq!(emit.get(&2, 2), Some(i * 2));
        }
    }
}

use std::borrow::ToOwned;
use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::num::Wrapping;
use std::os::raw::{c_char, c_int, c_ulong};
mod ccompat;
mod threadpool;
use ccompat::carray::CArray;
use global_emit::GlobalEmit;
use lazy_static::lazy_static;
use threadpool::{ReducePool, ThreadPool};

mod global_emit {
    use dashmap::DashMap;
    use std::borrow::{Borrow, ToOwned};
    use std::cell::UnsafeCell;
    use std::collections::BinaryHeap;
    use std::os::raw::{c_int, c_ulong};
    use std::sync::{Mutex, MutexGuard, RwLock};
    use std::{cmp::Eq, cmp::Reverse, hash::Hash};

    /// A Mutex protected value that can then be stabalized, yielding the the
    /// inner value. This is a useful construct almost exclusivly when statically
    /// alocating something with `lazy_static!`.
    enum StableMutex<T> {
        Unstable { inner: Mutex<Option<Box<T>>> },
        Stable { inner: Box<T> },
    }

    impl<T> StableMutex<T> {
        /// Wrapps the `inner` value in a `StableMutex`
        fn new(inner: T) -> Self {
            Self::Unstable {
                inner: Mutex::new(Some(Box::new(inner))),
            }
        }

        /// Stabalize the StableMutex. Removes the cost of aquiring the mutex.
        /// Calling `get_mut` or `stabalize` will now panic. You have been warned.
        fn stabalize(&self) {
            if let StableMutex::Unstable { inner } = self {
                let new = inner.lock().unwrap().take().unwrap();
                unsafe {
                    *(self as *const _ as *mut _) = Self::Stable { inner: new };
                }
            } else {
                panic!("Cannot stabalize an already stable StableMutex.")
            }
        }

        /// Get a mutable reference to a yet unstabalized StableMutex. If the
        /// mutex has already been stabalized, then will panic.
        fn get_mut(&self) -> MutexGuard<Option<Box<T>>> {
            if let Self::Unstable { inner } = self {
                inner.lock().unwrap()
            } else {
                panic!("You attempted to get a mutable reference to a stabalized value.")
            }
        }

        /// Get a refernce to the interior value of the stableized StableMutex.
        /// If the mutex has not yet been stabalized, panic.
        fn get_ref(&self) -> &T {
            if let Self::Stable { inner } = self {
                inner
            } else {
                panic!("You need to deal with the mutex before things are stabalized")
            }
        }

        /// Returns if the mutex has been stabalized.
        #[allow(dead_code)]
        fn is_stable(&self) -> bool {
            match self {
                Self::Unstable { inner: _ } => false,
                Self::Stable { inner: _ } => true,
            }
        }
    }

    impl<T> std::ops::Deref for StableMutex<T> {
        type Target = T;
        fn deref(&self) -> &Self::Target {
            self.get_ref()
        }
    }

    unsafe impl<T> std::marker::Sync for StableMutex<T> where T: std::marker::Sync {}

    /// DefaultDashMap is a RwLock protected DashMap that allows the threadsafe
    /// creation of multiple value containers (backed by a BinaryHeap).
    struct DefaultDashMap<K, V, Q>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
        K: Eq + Hash + Borrow<Q>,
    {
        internal: RwLock<DashMap<K, BinaryHeap<Reverse<V>>>>,
        q_bound: std::marker::PhantomData<Q>,
    }

    impl<K, V, Q> DefaultDashMap<K, V, Q>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
        K: Eq + Hash + Borrow<Q>,
        V: Ord,
    {
        /// Creates a new instance of `DefaultDashMap<K, V, Q>`
        fn new() -> Self {
            Self {
                internal: RwLock::new(DashMap::new()),
                q_bound: std::marker::PhantomData,
            }
        }

        /// Inserts a `value` into the `BinaryHeap` associated with the `key`
        /// given. This opperation is threadsafe.
        fn insert(&self, key: &Q, value: V) {
            let map_read = self.internal.read().unwrap(); // Allows inserting into a
                                                          // binary heap, but not creating a new heap.
            let alter_push = |map: &DashMap<K, _>, key, value| {
                map.alter(key, |_, mut v: BinaryHeap<_>| {
                    v.push(Reverse(value));
                    v
                })
            };
            if map_read.contains_key(key) {
                alter_push(&*map_read, key, value);
            } else {
                drop(map_read); // Prevent deadlock
                let map_write = self.internal.write().unwrap(); // necessary to write a new heap.
                if map_write.contains_key(key) {
                    // Sometimes another thread has written a new heap by this
                    // point, so we just add to that.
                    alter_push(&*map_write, key, value);
                } else {
                    // Otherwise we write a new heap, adding our element to
                    // that, and then insert the new heap.
                    let mut b = BinaryHeap::new();
                    b.push(Reverse(value));
                    map_write.insert(key.to_owned(), b);
                }
                // map_write drops
            }
            // map_read has droped by this point. It might have been earlier.
        }

        /// Pop a value associated with `key`. This value will be returned in
        /// order.
        fn get(&self, key: &Q) -> Option<V> {
            self.internal
                .read()
                .unwrap()
                .get_mut(key)
                .map(|mut m| m.pop().map(|v| v.0))
                .unwrap_or(None)
        }

        /// A vector of all keys currently in use.
        fn keys(&self) -> Vec<K> {
            let map: &DashMap<K, _> = &*self.internal.read().unwrap();
            let mut v: Vec<_> = Vec::with_capacity(map.len());
            map.iter()
                .map(|m| m.key().borrow().to_owned())
                .for_each(|k| v.push(k));
            v
        }
    }

    /// Encapsulates mutable global threadsafe state for a map between keys and
    /// values. There may be multiple values per key.
    ///
    /// The goal of this struct is to encapslate this state, allowing the rest of
    /// the code base to use normal and safe rust for a statically allocated
    /// mutating value.
    ///
    /// ```
    /// let emitted = GlobalEmit::new();
    /// emitted.emit("Foo", "Bar");
    /// emitted.emit("Fizz", "Buzz");
    /// assert!(emitted.get("Foo") == Some("Bar"));
    /// assert!(emitted.get("Foo") == None);
    /// ```
    pub struct GlobalEmit<K, V, Q: 'static, P: 'static>
    where
        K: Eq + Hash + Borrow<Q>,
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
    {
        internal: StableMutex<Vec<DefaultDashMap<K, V, Q>>>,
        partition: UnsafeCell<extern "C" fn(P, c_int) -> c_ulong>,
        pmask_key: &'static (dyn Fn(&Q) -> P),
    }

    unsafe impl<
            K: std::marker::Send + Eq + Hash + Borrow<Q>,
            V: std::marker::Send,
            Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
            P,
        > std::marker::Sync for GlobalEmit<K, V, Q, P>
    {
    }

    impl<K, V, Q, P> GlobalEmit<K, V, Q, P>
    where
        K: Eq + Hash + Borrow<Q> + std::cmp::Ord,
        V: std::cmp::Ord,
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
    {
        /// Emit a (key, value) pair into the GlobalEmit structure.
        pub fn emit(&self, key: &Q, value: V) {
            let inner = &self.internal;
            assert!(inner.len() > 0);
            let partition = unsafe { self.partition.get().as_ref().unwrap() };
            let map = &inner[partition((self.pmask_key)(key), inner.len() as c_int) as usize];
            map.insert(key, value);
        }

        /// Allocates an empty GlobalEmit structure with keys of type K and values
        /// of type V.
        pub fn new(
            partitioner: extern "C" fn(P, c_int) -> c_ulong,
            mask: &'static dyn Fn(&Q) -> P,
        ) -> GlobalEmit<K, V, Q, P> {
            GlobalEmit {
                internal: StableMutex::new(Vec::new()),
                partition: UnsafeCell::new(partitioner),
                pmask_key: mask,
            }
        }

        /// Setup the global emitter with `num_partition` partitions, using
        /// partition function `partition`. This function must be called before
        /// `emit` is called.
        pub fn setup(&self, partition: extern "C" fn(P, c_int) -> c_ulong, num_partition: usize) {
            {
                let mut guard = self.internal.get_mut();
                let internal: &mut Vec<_> = &mut (*guard).as_mut().unwrap();
                assert!(internal.len() == 0);
                unsafe { *self.partition.get().as_mut().unwrap() = partition };
                internal.extend((0..num_partition).map(|_| DefaultDashMap::new()));
                assert!(internal.len() != 0);
            }
            self.internal.stabalize();
        }

        /// Recieve the next value associated with key from the global emitter, or None if none is available.
        pub fn get(&self, key: &Q, partition_num: usize) -> Option<V> {
            let internal = &self.internal;
            let map = &internal[partition_num];
            map.get(key)
        }

        /// Returns an iterator of all keys in the given `partition`.
        ///
        /// The keys are returned in sorted order.
        ///
        /// ```
        /// for key in EMITTED.keys() {
        ///     println!("key: {:?}", key)
        /// }
        /// ```
        /// Example asumes `K: Debug`.
        pub fn keys(&self, partition: usize) -> impl Iterator<Item = K> {
            let part = &self.internal[partition];
            let mut v: Vec<_> = part.keys().into_iter().map(|k| Reverse(k)).collect();
            v.sort();
            v.into_iter().map(|k| k.0)
        }
    }
}

lazy_static! {
    // NOTE: this is not an Option<GlobalEmit> because that would require a
    // mutex to change, and we should avoid a long term solution that hides
    // everything behind a mutex.
    static ref EMITTED: GlobalEmit<CString, CString, CStr, *const c_char> =
        GlobalEmit::new(MR_DefaultHashPartition, &|cstr: &CStr| cstr.as_ptr());
}

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[no_mangle]
/// Emit a (key, value) pair into [`EMITTED`](EMITTED).
///
/// Memory manegment: [`MR_Emit`](MR_Emit) takes a copy of value, and key if it
/// needs it. The calling function is responsible for freeing both key and
/// value.
///
/// ```C
/// MR_Emit("Calling string", argv[0]);
/// ```
pub extern "C" fn MR_Emit(key: *const c_char, value: *const c_char) {
    let skey = unsafe { CStr::from_ptr(key) };
    let svalue = unsafe { CStr::from_ptr(value).to_owned() };
    EMITTED.emit(skey, svalue)
}

type Mapper = extern "C" fn(*const c_char);
type Reducer = extern "C" fn(*const c_char, *const Getter, c_int);
type Getter = extern "C" fn(*const c_char, c_int) -> *const c_char;
type Partitioner = extern "C" fn(*const c_char, c_int) -> c_ulong;

struct ThreadSafe<T>(T);
unsafe impl<T> std::marker::Sync for ThreadSafe<T> {}
unsafe impl<T> std::marker::Send for ThreadSafe<T> {}

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[no_mangle]
/// Entry point for the map-reduce library.
///
/// Takes a map function, a reduce function, and a partition function, along
/// with a thread-count for mappers and reducers. It computes maps and reduces,
/// with each reducer handling keys in one partition.
///
/// ```
/// #include <assert.h>
/// #include <stdio.h>
/// #include <stdlib.h>
/// #include <string.h>
/// #include "mapreduce.h"
///
/// void Map(char *file_name) {
///     FILE *fp = fopen(file_name, "r");
///     assert(fp != NULL);
///
///     char *line = NULL;
///     size_t size = 0;
///     while (getline(&line, &size, fp) != -1) {
///         char *token, *dummy = line;
///         while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
///             MR_Emit(token, "1");
///         }
///     }
///     free(line);
///     fclose(fp);
/// }
///
/// void Reduce(char *key, Getter get_next, int partition_number) {
///     int count = 0;
///     char *value;
///     while ((value = get_next(key, partition_number)) != NULL)
///         count++;
///     printf("%s %d\n", key, count);
/// }
///
/// int main(int argc, char *argv[]) {
///     MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
/// }
/// ```
/// This prints the number of words in each file.
pub extern "C" fn MR_Run(
    argc: c_int,
    argv: *const *const c_char,
    map: Mapper,
    num_mappers: c_int,
    reduce: Reducer,
    num_reducers: c_int,
    partition: Partitioner,
) {
    if num_mappers < 1 || num_reducers < 1 {
        println!(
            "There must be at least 1 mapper and at least one reducer.
                  There were {:?} mappers and {:?} reducers.",
            num_mappers, num_reducers
        );
        return;
    }

    // Must be first call to EMITTED
    EMITTED.setup(partition, num_reducers.try_into().unwrap());
    let mappers = ThreadPool::new(num_mappers as usize).unwrap();
    let file_names = CArray::from(argv);
    for name in file_names.iter_to(argc as usize).skip(1) {
        let file_ptr = ThreadSafe(*name);
        mappers.execute(move || map(file_ptr.0));
    }
    mappers.join(); // Join all threads

    // Where ReducePool is a threadpool that will run reducer(key, getter, partition(key))
    let mut reduce_pool = ReducePool::new();
    for (i, keys) in (0..num_reducers).map(|i| (i, EMITTED.keys(i as usize))) {
        reduce_pool.execute(keys.map(move |k| move || reduce(k.as_ptr(), getter as _, i)));
    }
    reduce_pool.join();
}

#[no_mangle]
/// Get the next `value` associated with `key` in `partition_number` or NULL if no key exists.
pub extern "C" fn getter(key: *const c_char, partition_number: c_int) -> *const c_char {
    EMITTED
        .get(unsafe { CStr::from_ptr(key) }, partition_number as usize)
        .map(|s| s.as_ptr())
        .unwrap_or(0 as *const c_char)
}

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[no_mangle]
/// Hashes `key` into `num_partitions`. This is a simple hash function ported
/// directly from the ostep text book.
///
/// ```C
/// MR_DefaultHashPartition("Foo", 32) // some value in [0,32)
/// ```
pub extern "C" fn MR_DefaultHashPartition(key: *const c_char, num_partitions: c_int) -> c_ulong {
    let mut hash: Wrapping<c_ulong> = Wrapping(5381);
    let mut c: c_char;
    let mut offset = 0;
    while {
        c = unsafe { *key.offset(offset) };
        c
    } != '\0' as c_char
    {
        offset += 1;
        hash = hash * Wrapping(33) + Wrapping(c as c_ulong);
    }
    hash.0 % (num_partitions as c_ulong)
}
