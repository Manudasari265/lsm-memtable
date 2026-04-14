use crate::entry::Entry;
use std::sync::atomic::AtomicPtr;

pub struct Node {
    pub entry: Option<Entry>, // actual data stored in the node, Option is for head node 
    pub tower: Box<[AtomicPtr<Node>]>, // array of pointers to next nodes at each level 
}