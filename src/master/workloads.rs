
/// Enum representing the possible patterns of transmission
/// for source nodes in a test.
#[derive(Debug, Deserialize, PartialEq)]
pub enum SourceProfiles {
    ///Constant-Bitrate. Params are: 
    /// Destination
    /// Packets per second
    /// Packet Size
    /// Duration in milliseconds of transmission
    CBR(String, usize, usize, u64),
}