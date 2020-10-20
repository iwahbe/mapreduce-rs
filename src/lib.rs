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

use std::borrow::{Borrow, ToOwned};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_ulong};
use std::sync::Mutex;
use std::{cmp::Eq, hash::Hash};
mod ccompat;
mod threadpool;
use ccompat::carray::CArray;
use lazy_static::lazy_static;
use threadpool::ThreadPool;

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
struct GlobalEmit<K, V, Q: 'static, P: 'static>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
{
    internal: UnsafeCell<Vec<Mutex<HashMap<K, Vec<V>>>>>,
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
    K: Eq + Hash + Borrow<Q>,
    Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
{
    /// Emit a (key, value) pair into the GlobalEmit structure.
    fn emit(&self, key: &Q, value: V) {
        let inner: &mut Vec<_> = unsafe { self.internal.get().as_mut().unwrap() };
        assert!(inner.len() > 0);
        let partition = self.partition.get();
        inner[(unsafe { *partition })((self.pmask_key)(key), inner.len() as c_int) as usize]
            .lock()
            .map(|mut m| match m.get_mut(key) {
                Some(v) => v.push(value),
                None => {
                    m.insert(key.to_owned(), vec![value]);
                }
            })
            .unwrap()
    }

    /// Allocates an empty GlobalEmit structure with keys of type K and values
    /// of type V.
    fn new(
        partitioner: extern "C" fn(P, c_int) -> c_ulong,
        mask: &'static dyn Fn(&Q) -> P,
    ) -> GlobalEmit<K, V, Q, P> {
        GlobalEmit {
            internal: UnsafeCell::new(Vec::new()),
            partition: UnsafeCell::new(partitioner),
            pmask_key: mask,
        }
    }

    /// The number of values in the emitter.
    fn count(&self) -> usize {
        let inner: &mut Vec<_> = unsafe { self.internal.get().as_mut().unwrap() };
        inner.iter().fold(0, |count: usize, hmap| {
            hmap.lock().unwrap().values().fold(0, |c, i| c + i.len()) + count
        })
    }

    /// Setup the global emitter with `num_partition` partitions, using
    /// partition function `partition`. This function must be called before
    /// `emit` is called.
    fn setup(&self, partition: extern "C" fn(P, c_int) -> c_ulong, num_partition: usize) {
        let internal = unsafe { self.internal.get().as_mut().unwrap() };
        assert!(internal.len() == 0);
        unsafe { *self.partition.get().as_mut().unwrap() = partition };
        *internal = (0..num_partition)
            .map(|_| Mutex::new(HashMap::new()))
            .collect();
    }

    /// Recieve the next value associated with key from the global emitter, or None if none is available.
    fn get(&self, key: &Q, partition_num: usize) -> Option<V> {
        let internal = unsafe { self.internal.get().as_mut().unwrap() };
        internal[partition_num]
            .lock()
            .map(|mut m| match m.get_mut(key) {
                Some(v) => v.pop(),
                None => None,
            })
            .unwrap_or(None)
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
///
/// To instead print the number of words found in all files, we can write
/// ```C
/// void Reduce(char *key, Getter get_next, int partition_number) {
///     int count = 0;
///     char *value;
///     while ((value = get_next(key, partition_number)) != NULL)
///         count += atoi(value);
///     char *res = (char*)malloc(sizeof(char)*10);
///     if (!strcmp(key, "TOTAL_COUNT")) {
///         printf("Total count %d", count)
///     } else {
///         snprintf(res, 10, "%d", count);
///         MR_Emit("TOTAL_COUNT", value);
///     }
/// }
/// ```
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
    EMITTED.setup(partition, num_reducers.try_into().unwrap());
    let mappers = ThreadPool::new(num_mappers as usize).unwrap();
    let file_names = CArray::from(argv);
    for name in file_names.iter_to(argc as usize).skip(1) {
        let file_ptr = ThreadSafe(*name);
        mappers.execute(move || map(file_ptr.0));
    }
    mappers.wait(0); // wait until there are no threads
    let _reducers = ThreadPool::new(num_reducers as usize).unwrap();
    for (i, name) in file_names.iter_to(argc as usize).enumerate().skip(1) {
        unsafe {
            println!("argv[{}]: {:?}", i, CStr::from_ptr(*name));
        }
    }
    println!(
        "num_mappers, num_reducers: {:?}, {:?}",
        num_mappers as u64, num_reducers as i64
    );
    let file_name = CString::new("my map file_name").unwrap();
    let map_ptr = file_name.as_bytes_with_nul().as_ptr();
    map(map_ptr as *const c_char);
    let reduce_key = CString::new("reduce key").unwrap();
    let reduce_key_ptr = reduce_key.as_bytes_with_nul().as_ptr();
    reduce(
        reduce_key_ptr as *const c_char,
        getter as *const extern "C" fn(*const c_char, c_int) -> *const c_char,
        7,
    );

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
