use crate::entry::Entry;
use std::sync::atomic::AtomicPtr;

pub struct Node {
    pub entry: Option<Entry>, // actual data stored in the node, Option is for head node
    pub tower: Box<[AtomicPtr<Node>]>, // array of pointers to next nodes at each level
}

impl Node {
    pub fn new(entry: Option<Entry>, height: usize) -> *mut Node {
        // build tower: a vector of 'height' AtomicPtrs initialized to null
        let mut tower = Vec::with_capacity(height);
        
        for _ in 0..height {
            tower.push(AtomicPtr::new(std::ptr::null_mut()));
        }

        // convert vec into Box<[AtomicPtr<Node>]>
        let tower = tower.into_boxed_slice();
        let node = Node { entry, tower };

        // converts the Box<Node> into a raw pointer, which is what we need for the skip list structure
        Box::into_raw(Box::new(node))
    }
}
