use std::marker::PhantomData;

use anyhow::bail;

use crate::AnyError;

/// A map implementation where the key is a simple numeric index into a vector.
///
/// Allows for very fast lookups that can not fail.
/// Entries can not be deleted.
#[derive(Clone, Debug)]
pub struct StableMap<K, V> {
    values: Vec<V>,
    _key: PhantomData<K>,
}

pub trait StableMapKey {
    fn from_index(index: usize) -> Self;
    fn as_index(self) -> usize;
}

impl<K, V> StableMap<K, V>
where
    K: StableMapKey,
{
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            _key: PhantomData,
        }
    }

    /* /// Insert a new entry.
    /// Returns the key used for access.
    pub fn insert(&mut self, value: V) -> K {
        let index = self.values.len();
        self.values.push(value);
        K::from_index(index)
    } */

    /// Insert a new entry with a function that takes the new key as an argument.
    /// Useful when the key is required for initializing the value.
    pub fn insert_with(&mut self, f: impl FnOnce(K) -> V) -> K {
        let index = self.values.len();
        let value = f(K::from_index(index));
        self.values.push(value);
        K::from_index(index)
    }

    /// Number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Get a reference the value for the given key.
    ///
    /// Can not fail since entries can not be removed.
    #[inline]
    pub fn get(&self, key: K) -> &V {
        &self.values[key.as_index()]
    }

    /// Get a mutable reference to the value for the given key.
    ///
    /// Can not fail since entries can not be removed.
    #[inline]
    pub fn get_mut(&mut self, key: K) -> &mut V {
        &mut self.values[key.as_index()]
    }

    /// Iterate over the entries.
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<V> {
        self.values.iter()
    }

    /* /// Iterate over mutable entries.
    #[inline]
    pub fn iter_mut(&mut self) -> std::slice::IterMut<V> {
        self.values.iter_mut()
    } */
}

// TODO: Figure out how to represent this better.
// Currently not a great abstraction because it can lead to panics. 
pub struct DerivedStableMap<K, V>(StableMap<K, V>);

impl<K, V> DerivedStableMap<K, V>
where
    K: StableMapKey,
{
    pub fn new() -> Self {
        Self(StableMap::new())
    }

    /// Number of entries.
    #[inline]
    // TODO: remove.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.0.values.len()
    }

    /// Get a reference the value for the given key.
    ///
    /// Can not fail since entries can not be removed.
    #[inline]
    pub fn get(&self, key: K) -> &V {
        self.0.get(key)
    }

    /// Get a mutable reference to the value for the given key.
    ///
    /// Can not fail since entries can not be removed.
    #[inline]
    pub fn get_mut(&mut self, key: K) -> &mut V {
        self.0.get_mut(key)
    }

    /* /// Iterate over the entries.
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<V> {
        self.0.iter()
    } */

    /* /// Iterate over mutable entries.
    #[inline]
    pub fn iter_mut(&mut self) -> std::slice::IterMut<V> {
        self.0.iter_mut()
    } */

    // TODO: use custom error type.
    pub fn insert(&mut self, key: K, value: V) -> Result<(), AnyError> {
        if self.0.len() != key.as_index() {
            bail!("DerivedStableMap consistency violated");
        }
        self.0.values.push(value);
        Ok(())
    }
}
