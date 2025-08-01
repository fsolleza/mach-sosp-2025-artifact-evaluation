use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::str::from_utf8;

pub trait FlatBytes {
	fn write_to_vec(&self, data: &mut Vec<u8>);
	fn write_to_slice(&self, data: &mut [u8]) -> usize;
	fn read_from_slice(data: &[u8]) -> (Self, usize)
	where
		Self: Sized;
}

impl FlatBytes for u64 {
	fn write_to_vec(&self, data: &mut Vec<u8>) {
		data.extend_from_slice(&self.to_be_bytes());
	}

	fn write_to_slice(&self, data: &mut [u8]) -> usize {
		data[..8].copy_from_slice(&self.to_be_bytes());
		8
	}

	fn read_from_slice(data: &[u8]) -> (Self, usize) {
		let x = u64::from_be_bytes(data[..8].try_into().unwrap());
		(x, 8)
	}
}

impl FlatBytes for String {
	fn write_to_vec(&self, data: &mut Vec<u8>) {
		let bytes = self.as_bytes();
		data.extend_from_slice(&bytes.len().to_be_bytes());
		data.extend_from_slice(bytes);
	}

	fn write_to_slice(&self, data: &mut [u8]) -> usize {
		let bytes = self.as_bytes();
		let len = bytes.len();
		data[..8].copy_from_slice(&len.to_be_bytes());
		data[8..8 + len].copy_from_slice(bytes);
		8 + len
	}

	fn read_from_slice(data: &[u8]) -> (Self, usize) {
		let len = usize::from_be_bytes(data[..8].try_into().unwrap());
		let result = String::from(from_utf8(&data[8..8 + len]).unwrap());
		(result, len + 8)
	}
}

impl<T> FlatBytes for Vec<T>
where
	T: FlatBytes + Sized,
{
	fn write_to_vec(&self, data: &mut Vec<u8>) {
		data.extend_from_slice(&self.len().to_be_bytes());
		for item in self {
			item.write_to_vec(data)
		}
	}

	fn write_to_slice(&self, data: &mut [u8]) -> usize {
		data[..8].copy_from_slice(&self.len().to_be_bytes());
		let mut idx = 8;
		for item in self {
			let w = item.write_to_slice(&mut data[idx..]);
			idx += w;
		}
		idx
	}

	fn read_from_slice(data: &[u8]) -> (Self, usize) {
		let len = usize::from_be_bytes(data[..8].try_into().unwrap());
		let mut read = 8;
		let mut v = Vec::new();
		for _ in 0..len {
			let (i, r) = T::read_from_slice(&data[read..]);
			v.push(i);
			read += r;
		}
		(v, read)
	}
}

impl<K, V> FlatBytes for BTreeMap<K, V>
where
	K: FlatBytes + Ord,
	V: FlatBytes,
{
	fn write_to_vec(&self, data: &mut Vec<u8>) {
		data.extend_from_slice(&self.len().to_be_bytes());
		for (k, v) in self.iter() {
			k.write_to_vec(data);
			v.write_to_vec(data);
		}
	}

	fn write_to_slice(&self, data: &mut [u8]) -> usize {
		data[..8].copy_from_slice(&self.len().to_be_bytes());
		let mut idx = 8;
		for (k, v) in self.iter() {
			idx += k.write_to_slice(&mut data[idx..]);
			idx += v.write_to_slice(&mut data[idx..]);
		}
		idx
	}

	fn read_from_slice(data: &[u8]) -> (Self, usize) {
		let len = usize::from_be_bytes(data[..8].try_into().unwrap());
		let mut read = 8;
		let mut map = BTreeMap::new();
		for _ in 0..len {
			let (k, r) = K::read_from_slice(&data[read..]);
			read += r;
			let (v, r) = V::read_from_slice(&data[read..]);
			read += r;

			map.insert(k, v);
		}
		(map, read)
	}
}

impl<K, V> FlatBytes for HashMap<K, V>
where
	K: FlatBytes + Eq + Hash,
	V: FlatBytes,
{
	fn write_to_vec(&self, data: &mut Vec<u8>) {
		data.extend_from_slice(&self.len().to_be_bytes());
		for (k, v) in self.iter() {
			k.write_to_vec(data);
			v.write_to_vec(data);
		}
	}

	fn write_to_slice(&self, data: &mut [u8]) -> usize {
		data[..8].copy_from_slice(&self.len().to_be_bytes());
		let mut idx = 8;
		for (k, v) in self.iter() {
			idx += k.write_to_slice(&mut data[idx..]);
			idx += v.write_to_slice(&mut data[idx..]);
		}
		idx
	}

	fn read_from_slice(data: &[u8]) -> (Self, usize) {
		let len = usize::from_be_bytes(data[..8].try_into().unwrap());
		let mut read = 8;
		let mut map = HashMap::new();
		for _ in 0..len {
			let (k, r) = K::read_from_slice(&data[read..]);
			read += r;
			let (v, r) = V::read_from_slice(&data[read..]);
			read += r;

			map.insert(k, v);
		}
		(map, read)
	}
}
