#![warn(missing_docs)]

//! Implements a process based map-reduce library, made to link with a C header
//! `mapreduce.h`. This discourages an idiomatic rust approach. Because
//! [`Getter`](Getter) and [`MR_Emit`](MR_Emit) do not take any arguments for
//! managing state, this necessitates a mutable global [`EMITTED`](EMITTED)
//! object.

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

use std::borrow::ToOwned;
use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_ulong};
mod ccompat;
mod threadpool;
use ccompat::carray::CArray;
use global_emit::GlobalEmit;
use lazy_static::lazy_static;
use threadpool::{ReducePool, ThreadPool};

mod global_emit {
    use std::borrow::{Borrow, ToOwned};
    use std::cell::UnsafeCell;
    use std::collections::{BinaryHeap, HashMap};
    use std::os::raw::{c_int, c_ulong};
    use std::sync::Mutex;
    use std::{cmp::Eq, hash::Hash};

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
        internal: UnsafeCell<Vec<Mutex<HashMap<K, BinaryHeap<V>>>>>,
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
            let inner: &mut Vec<_> = unsafe { self.internal.get().as_mut().unwrap() };
            assert!(inner.len() > 0);
            let partition = self.partition.get();
            inner[(unsafe { *partition })((self.pmask_key)(key), inner.len() as c_int) as usize]
                .lock()
                .map(|mut m| match m.get_mut(key) {
                    Some(v) => v.push(value),
                    None => {
                        let mut b = BinaryHeap::new();
                        b.push(value);
                        m.insert(key.to_owned(), b);
                    }
                })
                .unwrap()
        }

        /// Allocates an empty GlobalEmit structure with keys of type K and values
        /// of type V.
        pub fn new(
            partitioner: extern "C" fn(P, c_int) -> c_ulong,
            mask: &'static dyn Fn(&Q) -> P,
        ) -> GlobalEmit<K, V, Q, P> {
            GlobalEmit {
                internal: UnsafeCell::new(Vec::new()),
                partition: UnsafeCell::new(partitioner),
                pmask_key: mask,
            }
        }

        /// Setup the global emitter with `num_partition` partitions, using
        /// partition function `partition`. This function must be called before
        /// `emit` is called.
        pub fn setup(&self, partition: extern "C" fn(P, c_int) -> c_ulong, num_partition: usize) {
            let internal = unsafe { self.internal.get().as_mut().unwrap() };
            assert!(internal.len() == 0);
            unsafe { *self.partition.get().as_mut().unwrap() = partition };
            *internal = (0..num_partition)
                .map(|_| Mutex::new(HashMap::new()))
                .collect();
        }

        /// Recieve the next value associated with key from the global emitter, or None if none is available.
        pub fn get(&self, key: &Q, partition_num: usize) -> Option<V> {
            let internal = unsafe { self.internal.get().as_mut().unwrap() };
            internal[partition_num]
                .lock()
                .map(|mut m| match m.get_mut(key) {
                    Some(v) => v.pop(),
                    None => None,
                })
                .unwrap_or(None)
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
            let internal = unsafe { self.internal.get().as_ref().unwrap() };
            let part = &internal[partition];
            let mut v: Vec<_> = Vec::with_capacity(internal.len());
            for k in part.lock().unwrap().keys() {
                v.push(k.borrow().to_owned());
            }
            v.sort();
            v.into_iter()
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
    println!(
        "MR_Emit called with key: {:?} and value: {:?}",
        unsafe { CStr::from_ptr(key) },
        unsafe { CStr::from_ptr(value) }
    );
    let skey = unsafe { CStr::from_ptr(value) };
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
    mappers.wait(0); // wait until there are no threads

    // Where ReducePool is a threadpool that will run reducer(key, getter, partition(key))
    let reduce_pool = ReducePool::new();
    for (i, keys) in (0..num_mappers).map(|i| (i, EMITTED.keys(i as usize))) {
        reduce_pool.execute(keys.map(move |k| move || reduce(k.as_ptr(), getter as _, i)));
    }
    reduce_pool.wait(0);

    println!("MR_RUN called");
}

#[no_mangle]
/// Get the next `value` associated with `key` in `partition_number` or NULL if no key exists.
pub extern "C" fn getter(key: *const c_char, partition_number: c_int) -> *const c_char {
    println!(
        "getter called with key: {:?}, partition_number: {}",
        unsafe { CStr::from_ptr(key) },
        partition_number
    );
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
    let mut hash: c_ulong = 5381;
    let mut c: c_char;
    let mut offset = 0;
    while {
        c = unsafe { *key.offset(offset) };
        c
    } != '\0' as c_char
    {
        offset += 1;
        hash = hash * 33 + (c as c_ulong);
    }
    hash % (num_partitions as c_ulong)
}
