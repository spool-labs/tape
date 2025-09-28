use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FixedMap<K: Pod + Zeroable, V: Pod + Zeroable, const N: usize> {
    pub length: u64,
    pub keys: [K; N],
    pub values: [V; N],
}

unsafe impl<K: Pod + Zeroable, V: Pod + Zeroable, const N: usize> Zeroable for FixedMap<K, V, N> {}
unsafe impl<K: Pod + Zeroable, V: Pod + Zeroable, const N: usize> Pod for FixedMap<K, V, N> {}

impl<K, V, const N: usize> FixedMap<K, V, N>
where
    K: Ord + Copy + Pod + Zeroable,
    V: Copy + Pod + Zeroable,
{
    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.length as usize
    }

    /// Returns `true` if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears the map, removing all key-value pairs.
    /// Note: This sets length to 0; the arrays remain zeroed but unchanged otherwise.
    pub fn clear(&mut self) {
        self.length = 0;
    }

    /// Returns a reference to the value corresponding to the key, if it exists.
    pub fn get(&self, key: &K) -> Option<&V> {
        let len = self.len();
        match self.keys[..len].binary_search_by(|k| k.cmp(key)) {
            Ok(idx) => Some(&self.values[idx]),
            Err(_) => None,
        }
    }

    /// Returns a mutable reference to the value corresponding to the key, if it exists.
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let len = self.len();
        match self.keys[..len].binary_search_by(|k| k.cmp(key)) {
            Ok(idx) => Some(&mut self.values[idx]),
            Err(_) => None,
        }
    }

    /// Returns `true` if the map contains a value for the specified key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Inserts a key-value pair into the map.
    /// If the key already exists, the old value is returned.
    /// If the map is at capacity (length == N) and the key is new, returns Err((key, value)).
    /// Otherwise, Ok(old_value) where old_value is Some if overwritten, None if new.
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>, (K, V)> {
        let len = self.len();
        match self.keys[..len].binary_search_by(|k| k.cmp(&key)) {
            Ok(idx) => {
                // Key exists: overwrite value.
                let old = self.values[idx];
                self.values[idx] = value;
                Ok(Some(old))
            }
            Err(idx) => {
                // Key not found: insert at position idx.
                if len == N {
                    return Err((key, value)); // At capacity.
                }
                // Shift keys and values to the right.
                self.keys.copy_within(idx..len, idx + 1);
                self.values.copy_within(idx..len, idx + 1);
                self.keys[idx] = key;
                self.values[idx] = value;
                self.length += 1;
                Ok(None)
            }
        }
    }

    /// Removes a key from the map, returning the value at the key if the key was previously in the map.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let len = self.len();
        match self.keys[..len].binary_search_by(|k| k.cmp(key)) {
            Ok(idx) => {
                let old = self.values[idx];
                // Shift keys and values to the left.
                self.keys.copy_within(idx + 1..len, idx);
                self.values.copy_within(idx + 1..len, idx);
                self.length -= 1;
                Some(old)
            }
            Err(_) => None,
        }
    }

    /// An iterator visiting all key-value pairs in arbitrary order.
    /// The iterator element type is `(&'a K, &'a V)`.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        let len = self.len();
        self.keys[..len].iter().zip(self.values[..len].iter())
    }

    /// An iterator visiting all key-value pairs in arbitrary order, with mutable references to the values.
    /// The iterator element type is `(&'a K, &'a mut V)`.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> + '_ {
        let len = self.len();
        self.keys[..len].iter().zip(self.values[..len].iter_mut())
    }
}
