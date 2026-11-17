use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
#[repr(i16)]
pub enum KafkaErrorCode {
    Unknown = -1,
    NoError = 0,
    UnsupportedVersion = 35,
}
