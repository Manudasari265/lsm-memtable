use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{consts::MAX_HEIGHT, entry::Entry, skiplist::node::Node};

pub mod node;

pub struct SkipList {
    pub head: *mut Node,
    pub height: AtomicUsize,
    pub length: AtomicUsize,
}

impl SkipList {
    pub fn new() -> Self {
        let head = Node::new(None, MAX_HEIGHT);
        SkipList {
            head,
            height: AtomicUsize::new(1),
            length: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, entry: Entry) {
        let height = random_height();
        let new_node = Node::new(Some(entry), height);

        let mut preds = vec![self.head; MAX_HEIGHT];
        let mut current = self.head;

        for level in (0..self.height.load(Ordering::SeqCst)).rev() {
            loop {
                let next = unsafe { (*current).tower[level].load(Ordering::Acquire) };
              
                if next.is_null() {
                    preds[level] = current;
                    break;
                }

                let next_key = unsafe { (*next).entry.as_ref().unwrap().key.as_slice() };
                let new_node_key = unsafe { (*new_node).entry.as_ref().unwrap().key.as_slice() };  

                if next_key < new_node_key {
                    current = next;
                } else {
                    preds[level] = current;
                    break;
                }

            }
        }

        for level in 0..height {
            let next = unsafe { (*preds[level]).tower[level].load(Ordering::Acquire) };

            unsafe {
                (*new_node).tower[level].store(next, Ordering::Relaxed);
                (*preds[level]).tower[level].store(new_node, Ordering::Release);
            };
        }

        if height > self.height.load(Ordering::Relaxed) {
            self.height.store(height, Ordering::Relaxed);
        }

        self.length.fetch_add(1, Ordering::Relaxed);
    }
}

fn random_height() -> usize {
    let mut height = 1;

    while height < MAX_HEIGHT && rand::random::<f64>() < 0.5 {
        height += 1;
    }

    height
}
