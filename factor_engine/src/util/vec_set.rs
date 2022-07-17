/// A set implemeted with a vector.
///
/// Useful for very small sets where the hashing overhead exceeds iterating and
/// comparing the elements directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VecSet<T> {
    items: Vec<T>,
}

impl<T> Default for VecSet<T> {
    fn default() -> Self {
        Self { items: Vec::new() }
    }
}

impl<T: PartialEq + Eq> VecSet<T> {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn add(&mut self, item: T) {
        if !self.items.contains(&item) {
            self.items.push(item);
        }
    }

    pub fn remove(&mut self, item: &T) {
        self.items.retain(|x| x != item);
    }

    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        self.items.contains(value)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.items.iter()
    }
}

impl<T: PartialEq + Eq> FromIterator<T> for VecSet<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut items = Vec::new();
        for item in iter.into_iter() {
            if !items.contains(&item) {
                items.push(item);
            }
        }
        Self { items }
    }
}

impl<'a, T> IntoIterator for &'a VecSet<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}
