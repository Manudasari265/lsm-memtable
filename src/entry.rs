#[derive(Debug, Clone)]
pub enum ValueType {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub sequence_number: u64,
    pub value_type: ValueType,
}
