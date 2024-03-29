use std::marker::PhantomData;

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
    fn as_index(&self) -> usize;
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

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

impl<K, V> Default for StableMap<K, V>
where
    K: StableMapKey,
{
    fn default() -> Self {
        Self::new()
    }
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

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    pub fn insert(&mut self, value: V) -> K {
        let key = K::from_index(self.0.values.len());
        self.0.values.push(value);
        key
    }

    /// Append a new item.
    /// The key must be the correct index key for the next item, as it would be
    /// returned by [`Self::insert`].
    ///
    /// WARNING: panics if the key is not correct.
    ///
    pub fn append_checked(&mut self, key: K, value: V) -> K {
        if key.as_index() != self.0.values.len() {
            panic!(
                "Invalid stable map append: expected index key {}, but got {}",
                self.0.values.len(),
                key.as_index()
            );
        }
        self.0.values.push(value);
        key
    }
}

impl<K, V> Default for DerivedStableMap<K, V>
where
    K: StableMapKey,
{
    fn default() -> Self {
        Self::new()
    }
}
