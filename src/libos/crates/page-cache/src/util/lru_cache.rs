use std::{borrow::Borrow, boxed::Box, collections::HashMap, hash::Hash, ptr::NonNull};

struct Node<K, V> {
    k: K,
    v: V,
    prev: Option<NonNull<Node<K, V>>>,
    next: Option<NonNull<Node<K, V>>>,
}

struct KeyRef<K, V>(NonNull<Node<K, V>>);

impl<K: Hash + Eq, V> Borrow<K> for KeyRef<K, V> {
    fn borrow(&self) -> &K {
        unsafe { &self.0.as_ref().k }
    }
}

impl<K: Hash, V> Hash for KeyRef<K, V> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe { self.0.as_ref().k.hash(state) }
    }
}

impl<K: Eq, V> PartialEq for KeyRef<K, V> {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.0.as_ref().k.eq(&other.0.as_ref().k) }
    }
}

impl<K: Eq, V> Eq for KeyRef<K, V> {}

impl<K, V> Node<K, V> {
    fn new(k: K, v: V) -> Self {
        Self {
            k,
            v,
            prev: None,
            next: None,
        }
    }
}

pub struct LruCache<K, V> {
    head: Option<NonNull<Node<K, V>>>,
    tail: Option<NonNull<Node<K, V>>>,
    map: HashMap<KeyRef<K, V>, NonNull<Node<K, V>>>,
    cap: usize,
}

// Safety. K and V implement Send.
unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}

impl<K: Hash + Eq + PartialEq, V> LruCache<K, V> {
    pub fn new(cap: usize) -> Self {
        Self {
            head: None,
            tail: None,
            map: HashMap::new(),
            cap,
        }
    }

    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        let node = Box::leak(Box::new(Node::new(k, v))).into();
        let old_node = self.map.remove(&KeyRef(node)).map(|node| {
            self.detach(node);
            node
        });
        // Comment self-evict policy to avoid mis-behavior of lrucache
        // e.g., in page-cache, evict dirty pages without flushing.
        // if old_node.is_none() && self.map.len() >= self.cap {
        //     let tail = self.tail.unwrap();
        //     self.detach(tail);
        //     self.map.remove(&KeyRef(tail));
        // }
        self.attach(node);

        self.map.insert(KeyRef(node), node);
        old_node.map(|node| unsafe {
            let node = Box::from_raw(node.as_ptr());
            node.v
        })
    }

    pub fn get(&mut self, k: &K) -> Option<&V> {
        if let Some(node) = self.map.get(k) {
            let node = *node;
            self.detach(node);
            self.attach(node);
            unsafe { Some(&node.as_ref().v) }
        } else {
            None
        }
    }

    pub fn evict(&mut self) -> Option<V> {
        let tail = self.tail.unwrap();
        self.detach(tail);
        self.map.remove(&KeyRef(tail)).map(|node| unsafe {
            let node = Box::from_raw(node.as_ptr());
            node.v
        })
    }

    pub fn just_get(&self, k: &K) -> Option<&V> {
        if let Some(node) = self.map.get(k) {
            unsafe { Some(&node.as_ref().v) }
        } else {
            None
        }
    }

    pub fn size(&self) -> usize {
        self.map.len()
    }

    fn detach(&mut self, mut node: NonNull<Node<K, V>>) {
        unsafe {
            match node.as_mut().prev {
                Some(mut prev) => {
                    prev.as_mut().next = node.as_ref().next;
                }
                None => {
                    self.head = node.as_ref().next;
                }
            }
            match node.as_mut().next {
                Some(mut next) => {
                    next.as_mut().prev = node.as_ref().prev;
                }
                None => {
                    self.tail = node.as_ref().prev;
                }
            }

            node.as_mut().prev = None;
            node.as_mut().next = None;
        }
    }

    fn attach(&mut self, mut node: NonNull<Node<K, V>>) {
        match self.head {
            Some(mut head) => {
                unsafe {
                    head.as_mut().prev = Some(node);
                    node.as_mut().next = Some(head);
                }
                self.head = Some(node);
            }
            None => {
                self.head = Some(node);
                self.tail = Some(node);
            }
        }
    }
}

impl<K, V> Drop for LruCache<K, V> {
    fn drop(&mut self) {
        while let Some(node) = self.head.take() {
            unsafe {
                self.head = node.as_ref().next;
                drop(Box::from_raw(node.as_ptr()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_cache_with_manual_evict() {
        let mut cache = LruCache::<usize, usize>::new(2);
        cache.put(1, 1);
        cache.put(2, 2);
        assert_eq!(*cache.get(&1).unwrap(), 1);
        cache.put(3, 3);
        assert_eq!(cache.evict().unwrap(), 2);
        assert_eq!(cache.get(&2), None);
        cache.put(5, 5);
        assert_eq!(cache.evict().unwrap(), 1);
        assert_eq!(*cache.get(&3).unwrap(), 3);
        assert_eq!(*cache.get(&5).unwrap(), 5);
    }
}
