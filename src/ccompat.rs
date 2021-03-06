pub mod carray {
    #[no_mangle]
    #[repr(transparent)]
    /// Transparent wrapper on a C array to facilitate easy indexing and
    /// iteration.
    ///
    /// Does not take ownership of the memory it observes. Does not free memory.
    ///
    /// ```
    /// # fn get_c_array() { 0xFFF as *cont c_int }
    /// let base_array: *const c_int = get_c_array();
    /// base_array[5] // Does not compile
    /// let carray: CArray<_> = base_array.into();
    /// carray[5] // Compiles
    /// ```
    pub struct CArray<T> {
        head: *const T,
    }

    impl<T> From<*const T> for CArray<T> {
        fn from(head: *const T) -> Self {
            CArray { head }
        }
    }

    impl<T, S: Into<usize>> std::ops::Index<S> for CArray<T> {
        type Output = T;
        fn index(&self, ind: S) -> &Self::Output {
            let u: usize = ind.into();
            unsafe { self.head.offset(u as isize).as_ref().unwrap() }
        }
    }

    impl<T, S: Into<usize>> std::ops::IndexMut<S> for CArray<T> {
        fn index_mut(&mut self, ind: S) -> &mut Self::Output {
            let u: usize = ind.into();
            unsafe { (self.head as *mut T).offset(u as isize).as_mut().unwrap() }
        }
    }

    pub struct Iter<'a, T> {
        array: &'a CArray<T>,
        index: usize,
        max: usize,
    }

    impl<T> CArray<T> {
        pub fn iter_to<'a, S: Into<usize>>(&'a self, len: S) -> Iter<'a, T> {
            Iter {
                array: &self,
                index: 0,
                max: len.into(),
            }
        }
    }
    impl<'a, T> Iterator for Iter<'a, T> {
        type Item = &'a T;
        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.max {
                let out = &self.array[self.index];
                self.index += 1;
                Some(out)
            } else {
                None
            }
        }
    }
}

/// An unsafe wrapper around an object to allow it to go between threads.
#[repr(transparent)]
pub struct ThreadSafe<T>(T);
impl<T> ThreadSafe<T> {
    pub unsafe fn new(t: T) -> Self {
        ThreadSafe(t)
    }
}

impl<T> std::ops::Deref for ThreadSafe<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for ThreadSafe<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

unsafe impl<T> std::marker::Sync for ThreadSafe<T> {}
unsafe impl<T> std::marker::Send for ThreadSafe<T> {}
