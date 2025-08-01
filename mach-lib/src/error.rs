#[derive(Copy, Clone, Debug)]
pub enum MachError {
	ChunkTail,
	MaxAddress,
	UncommittedAddress,
	PartitionNotInReader,
}
