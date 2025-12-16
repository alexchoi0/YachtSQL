use std::ops::{Deref, DerefMut};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct StaticCell<T>(RwLock<T>);

impl<T> StaticCell<T> {
    pub const fn new(value: T) -> Self {
        Self(RwLock::new(value))
    }

    pub fn with<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        f(&*self.0.read().unwrap())
    }

    pub fn with_mut<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        f(&mut *self.0.write().unwrap())
    }

    pub fn set(&self, value: T) {
        *self.0.write().unwrap() = value;
    }
}

impl<T: Copy> StaticCell<T> {
    pub fn get_copy(&self) -> T {
        *self.0.read().unwrap()
    }
}

impl<T: Default> StaticCell<T> {
    pub fn take(&self) -> T {
        std::mem::take(&mut *self.0.write().unwrap())
    }
}

pub struct StaticRefCell<T>(RwLock<T>);

impl<T> StaticRefCell<T> {
    pub const fn new(value: T) -> Self {
        Self(RwLock::new(value))
    }

    pub fn with<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        f(&*self.0.read().unwrap())
    }

    pub fn with_mut<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        f(&mut *self.0.write().unwrap())
    }

    pub fn set(&self, value: T) {
        *self.0.write().unwrap() = value;
    }
}

impl<T: Clone> StaticRefCell<T> {
    pub fn clone_inner(&self) -> T {
        self.0.read().unwrap().clone()
    }
}

pub struct StaticRef<'a, T>(RwLockReadGuard<'a, T>);

impl<T> Deref for StaticRef<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

pub struct StaticRefMut<'a, T>(RwLockWriteGuard<'a, T>);

impl<T> Deref for StaticRefMut<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for StaticRefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> StaticRefCell<T> {
    pub fn borrow(&self) -> StaticRef<'_, T> {
        StaticRef(self.0.read().unwrap())
    }

    pub fn borrow_mut(&self) -> StaticRefMut<'_, T> {
        StaticRefMut(self.0.write().unwrap())
    }
}

impl<T: Default> StaticRefCell<T> {
    pub fn take(&self) -> T {
        std::mem::take(&mut *self.0.write().unwrap())
    }
}

pub struct LazyStaticRefCell<T, F = fn() -> T> {
    cell: RwLock<Option<T>>,
    init: F,
}

impl<T, F: Fn() -> T> LazyStaticRefCell<T, F> {
    pub const fn new(init: F) -> Self {
        Self {
            cell: RwLock::new(None),
            init,
        }
    }

    fn get_or_init<R, G: FnOnce(&T) -> R>(&self, f: G) -> R {
        {
            let guard = self.cell.read().unwrap();
            if let Some(ref val) = *guard {
                return f(val);
            }
        }
        {
            let mut guard = self.cell.write().unwrap();
            if guard.is_none() {
                *guard = Some((self.init)());
            }
            f(guard.as_ref().unwrap())
        }
    }

    fn get_or_init_mut<R, G: FnOnce(&mut T) -> R>(&self, f: G) -> R {
        let mut guard = self.cell.write().unwrap();
        if guard.is_none() {
            *guard = Some((self.init)());
        }
        f(guard.as_mut().unwrap())
    }

    pub fn with<R, G: FnOnce(&T) -> R>(&self, f: G) -> R {
        self.get_or_init(f)
    }

    pub fn with_mut<R, G: FnOnce(&mut T) -> R>(&self, f: G) -> R {
        self.get_or_init_mut(f)
    }
}
