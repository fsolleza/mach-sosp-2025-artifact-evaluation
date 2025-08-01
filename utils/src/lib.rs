use dashmap::{DashMap, DashSet};
use fxhash::FxBuildHasher;
use std::{cmp::Eq, hash::Hash, sync::Arc};

pub use fxhash::{hash32, hash64};
pub use std::time::{Duration, SystemTime};
pub type Instant = minstant::Instant;

#[derive(Clone)]
pub struct ConcurrentMap<K: Eq + Hash, V> {
	inner: Arc<DashMap<K, V, FxBuildHasher>>,
}

impl<K: Eq + Hash, V> ConcurrentMap<K, V> {
	pub fn new() -> Self {
		let hasher = FxBuildHasher::default();
		Self {
			inner: Arc::new(DashMap::with_hasher(hasher)),
		}
	}
}

impl<K: Eq + Hash, V> std::ops::Deref for ConcurrentMap<K, V> {
	type Target = DashMap<K, V, FxBuildHasher>;
	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

pub struct ConcurrentSet<K: Eq + Hash> {
	inner: Arc<DashSet<K, FxBuildHasher>>,
}

impl<K: Eq + Hash> ConcurrentSet<K> {
	pub fn new() -> Self {
		let hasher = FxBuildHasher::default();
		Self {
			inner: Arc::new(DashSet::with_hasher(hasher)),
		}
	}
}

impl<K: Eq + Hash> std::ops::Deref for ConcurrentSet<K> {
	type Target = DashSet<K, FxBuildHasher>;
	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

pub struct HashMap<K, V> {
	inner: std::collections::HashMap<K, V, FxBuildHasher>,
}

impl<K, V> HashMap<K, V> {
	pub fn new() -> Self {
		let hasher = FxBuildHasher::default();
		Self {
			inner: std::collections::HashMap::with_hasher(hasher),
		}
	}
}

impl<K, V> std::ops::Deref for HashMap<K, V> {
	type Target = std::collections::HashMap<K, V, FxBuildHasher>;
	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl<K, V> std::ops::DerefMut for HashMap<K, V> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.inner
	}
}

pub struct HashSet<K> {
	inner: std::collections::HashSet<K, FxBuildHasher>,
}

impl<K> HashSet<K> {
	pub fn new() -> Self {
		let hasher = FxBuildHasher::default();
		Self {
			inner: std::collections::HashSet::with_hasher(hasher),
		}
	}
}

impl<K> std::ops::Deref for HashSet<K> {
	type Target = std::collections::HashSet<K, FxBuildHasher>;
	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl<K> std::ops::DerefMut for HashSet<K> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.inner
	}
}

pub fn micros_since_epoch() -> u64 {
	SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_micros() as u64
}

pub fn u64_from_bytes(bytes: &[u8]) -> u64 {
	u64::from_be_bytes(bytes.try_into().unwrap())
}

pub fn u64_to_bytes(t: u64, bytes: &mut [u8]) {
	bytes.copy_from_slice(&t.to_be_bytes());
}

pub fn u64_to_byte_vec(t: u64, v: &mut Vec<u8>) {
	v.extend_from_slice(&t.to_be_bytes());
}

pub fn u32_from_bytes(bytes: &[u8]) -> u32 {
	u32::from_be_bytes(bytes.try_into().unwrap())
}

pub fn u32_to_bytes(t: u32, bytes: &mut [u8]) {
	bytes.copy_from_slice(&t.to_be_bytes());
}

pub fn u32_to_byte_vec(t: u32, v: &mut Vec<u8>) {
	v.extend_from_slice(&t.to_be_bytes());
}

pub fn f64_from_bytes(bytes: &[u8]) -> f64 {
	f64::from_be_bytes(bytes.try_into().unwrap())
}

pub fn f64_to_bytes(t: f64, bytes: &mut [u8]) {
	bytes.copy_from_slice(&t.to_be_bytes());
}

pub fn f64_to_byte_vec(t: f64, v: &mut Vec<u8>) {
	v.extend_from_slice(&t.to_be_bytes());
}
