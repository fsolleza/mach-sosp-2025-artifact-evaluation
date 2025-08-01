#[derive(Copy, Clone, Debug, Default)]
pub enum FieldKind {
	Uint,
	Bytes,

	//Int,
	//Float,
	#[default]
	None,
}

//#[derive(Copy, Clone, Debug, Default)]
//struct SchemaField {
//	kind: FieldKind,
//	offset: usize,
//}

#[derive(Clone, Default)]
pub enum RecordField {
	Uint(u64),
	Bytes(Vec<u8>),

	//Int(i64),
	//Float(f64),
	#[default]
	None,
}

impl RecordField {
	const NONE: RecordField = RecordField::None;

	pub fn as_u64(&self) -> Option<u64> {
		match self {
			Self::Uint(x) => Some(*x),
			_ => None,
		}
	}
}

#[derive(Copy, Clone, Debug)]
pub struct Schema {
	fields: [FieldKind; 128],
	nfields: usize,
}

impl Schema {
	pub fn new(field_kinds: &[FieldKind]) -> Self {
		let mut fields = [FieldKind::None; 128];
		let mut offset = 0;
		fields[0..field_kinds.len()].copy_from_slice(field_kinds);
		Self {
			fields,
			nfields: field_kinds.len(),
		}
	}

	pub fn get_field(&self, bytes: &[u8], idx: usize) -> RecordField {
		Record::get_field(bytes, idx, self)
	}
}

pub struct Record {
	data: [RecordField; 128],
	nfields: usize,
}

impl Record {
	pub fn new_empty() -> Self {
		let data = [RecordField::NONE; 128];
		Self { data, nfields: 0 }
	}

	pub fn set_field(&mut self, idx: usize, item: RecordField) {
		self.data[idx] = item;
	}

	pub fn add_field(&mut self, item: RecordField) {
		self.data[self.nfields] = item;
		self.nfields += 1;
	}

	pub fn as_bytes(&self, data: &mut [u8]) {
		let mut offset = 0;

		// Variable length fields start after the fixed length 8-byte fields;
		let mut var_offset = self.nfields * 8;

		//// Write number of items
		//data[offset..offset + 8].copy_from_slice(&self.nfields.to_be_bytes());
		//offset += 8;

		// Write every field
		for i in 0..self.nfields {
			match &self.data[i] {
				RecordField::Uint(x) => {
					data[offset..offset + 8].copy_from_slice(&x.to_be_bytes());
					offset += 8;
				}

				RecordField::Bytes(x) => {
					// Write the offset to the actual data
					data[offset..offset + 8]
						.copy_from_slice(&var_offset.to_be_bytes());

					// Write the length
					data[var_offset..var_offset + 8]
						.copy_from_slice(&x.len().to_be_bytes());
					var_offset += 8;

					// Write the data
					data[var_offset..var_offset + x.len()].copy_from_slice(x);
					var_offset += x.len();

					offset += 8;
				}

				RecordField::None => panic!("Found a None field"),
			}
		}
	}

	pub fn from_bytes(bytes: &[u8], schema: &Schema) -> Self {
		let mut offset = 0;

		let mut record_fields = [RecordField::NONE; 128];

		for idx in 0..schema.nfields {
			match schema.fields[idx] {
				FieldKind::Uint => {
					let val = u64::from_be_bytes(
						bytes[offset..offset + 8].try_into().unwrap(),
					);
					record_fields[idx] = RecordField::Uint(val);
					offset += 8;
				}

				FieldKind::Bytes => {
					let mut var_offset = usize::from_be_bytes(
						bytes[offset..offset + 8].try_into().unwrap(),
					);
					let len = usize::from_be_bytes(
						bytes[var_offset..var_offset + 8].try_into().unwrap(),
					);
					var_offset += 8;
					let bytes: Vec<u8> =
						bytes[var_offset..var_offset + len].into();
					record_fields[idx] = RecordField::Bytes(bytes);
					offset += 8;
				}

				FieldKind::None => panic!("Found a None field"),
			}
		}
		Self {
			data: record_fields,
			nfields: schema.nfields,
		}
	}

	pub fn get_field(bytes: &[u8], idx: usize, schema: &Schema) -> RecordField {
		let mut offset = idx * 8;
		let field = schema.fields[idx];
		match field {
			FieldKind::Uint => {
				let item = u64::from_be_bytes(
					bytes[offset..offset + 8].try_into().unwrap(),
				);
				RecordField::Uint(item)
			}

			FieldKind::Bytes => {
				// Read the offset to the actual data
				let mut var_offset = usize::from_be_bytes(
					bytes[offset..offset + 8].try_into().unwrap(),
				);

				// Read the length
				let len = usize::from_be_bytes(
					bytes[var_offset..var_offset + 8].try_into().unwrap(),
				);
				var_offset += 8;

				// Read the data
				let data: Vec<u8> = bytes[var_offset..var_offset + len].into();
				RecordField::Bytes(data)
			}

			FieldKind::None => panic!("Found a None field"),
		}
	}
}
