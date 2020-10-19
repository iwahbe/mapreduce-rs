#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_ulong};
use std::sync::{Mutex, MutexGuard};
mod ccompat;
mod threadpool;
use ccompat::carray::CArray;
use lazy_static::lazy_static;
use std::cell::UnsafeCell;
use threadpool::ThreadPool;

struct GlobalEmit<K, V> {
    keys: Mutex<UnsafeCell<HashMap<K, Vec<V>>>>,
}

// Should take key: AsRef<K> but CStr doesn't seem to implement AsRef<CString>
impl<K: std::cmp::Eq + std::hash::Hash + std::borrow::ToOwned<Owned = K>, V> GlobalEmit<K, V> {
    fn emit(&self, key: &K, value: V) {
        self.keys
            .lock()
            .map(|c: MutexGuard<_>| unsafe { (*c).get().as_mut().unwrap() })
            .map(|m: &mut HashMap<K, Vec<V>>| match m.get_mut(key) {
                Some(v) => v.push(value),
                None => {
                    m.insert(key.to_owned(), vec![value]);
                }
            })
            .unwrap()
    }

    fn new() -> GlobalEmit<K, V> {
        GlobalEmit {
            keys: Mutex::new(UnsafeCell::new(HashMap::new())),
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.keys
            .lock()
            .map(|c: MutexGuard<_>| unsafe { (*c).get().as_mut().unwrap() })
            .map(|m| match m.get_mut(key) {
                Some(v) => v.pop(),
                None => None,
            })
            .unwrap_or(None)
    }
}

lazy_static! {
    static ref EMITTED: GlobalEmit<CString, CString> = GlobalEmit::new();
}

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn MR_Emit(key: *const c_char, value: *const c_char) {
    println!(
        "MR_Emit called with key: {:?} and value: {:?}",
        unsafe { CStr::from_ptr(key) },
        unsafe { CStr::from_ptr(value) }
    );
    unsafe {
        EMITTED.emit(
            &CString::from(CStr::from_ptr(value)),
            CString::from(CStr::from_ptr(value)),
        )
    }
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
pub extern "C" fn MR_Run(
    argc: c_int,
    argv: *const *const c_char,
    map: Mapper,
    num_mappers: c_int,
    reduce: Reducer,
    num_reducers: c_int,
    _partition: Partitioner,
) {
    if num_mappers < 1 || num_reducers < 1 {
        println!(
            "There must be at least 1 mapper and at least one reducer.
                  There were {:?} mappers and {:?} reducers.",
            num_mappers, num_reducers
        );
        return;
    }
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
pub extern "C" fn getter(key: *const c_char, partition_number: c_int) -> *const c_char {
    println!(
        "getter called with key: {:?}, partition_number: {}",
        unsafe { CStr::from_ptr(key) },
        partition_number
    );
    EMITTED
        .get(unsafe { &CString::from(CStr::from_ptr(key)) })
        .map(|s| s.as_ptr())
        .unwrap_or(0 as *const c_char)
}

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[no_mangle]
/// Hash function ported from project description in ostep
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
