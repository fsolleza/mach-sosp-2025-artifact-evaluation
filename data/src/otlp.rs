mod opentelemetry {
	pub mod proto {
		pub mod common {
			pub mod v1 {
				include!("../pb-rs/opentelemetry.proto.common.v1.rs");
			}
		}
		pub mod resource {
			pub mod v1 {
				include!("../pb-rs/opentelemetry.proto.resource.v1.rs");
			}
		}
		pub mod trace {
			pub mod v1 {
				include!("../pb-rs/opentelemetry.proto.trace.v1.rs");
			}
		}
		pub mod logs {
			pub mod v1 {
				include!("../pb-rs/opentelemetry.proto.logs.v1.rs");
			}
		}
		pub mod metrics {
			pub mod v1 {
				include!("../pb-rs/opentelemetry.proto.metrics.v1.rs");
			}
		}
		pub mod collector {
			pub mod trace {
				pub mod v1 {
					include!(
						"../pb-rs/opentelemetry.proto.collector.trace.v1.rs"
					);
				}
			}
			pub mod logs {
				pub mod v1 {
					include!(
						"../pb-rs/opentelemetry.proto.collector.logs.v1.rs"
					);
				}
			}
			pub mod metrics {
				pub mod v1 {
					include!(
						"../pb-rs/opentelemetry.proto.collector.metrics.v1.rs"
					);
				}
			}
		}
	}
}

pub use self::opentelemetry::proto::*;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OtlpData {
	pub resource: Resource,
	pub scope: InstrumentationScope,
	pub data: OtlpType,
	pub timestamp_us: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct OtlpDataset {
	pub data: Vec<OtlpData>,
}

impl OtlpDataset {
	pub fn new() -> Self {
		OtlpDataset { data: Vec::new() }
	}
}

impl<T: FlattenOtlp> From<T> for OtlpDataset {
	fn from(data: T) -> Self {
		let mut v = OtlpDataset::new();
		data.flatten_into(&mut v);
		v
	}
}

impl std::ops::Deref for OtlpDataset {
	type Target = Vec<OtlpData>;
	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl std::ops::DerefMut for OtlpDataset {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.data
	}
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum OtlpType {
	Metric(Metric),
	LogRecord(logs::v1::LogRecord),
	Span(trace::v1::Span),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Metric {
	GaugePoint {
		point: metrics::v1::NumberDataPoint,
	},
	SumPoint {
		point: metrics::v1::NumberDataPoint,
		aggregation_temporality: u64,
		is_monotonic: bool,
	},
	HistogramPoint {
		point: metrics::v1::HistogramDataPoint,
		aggregation_temporality: u64,
	},
	ExponentialHistogramPoint {
		point: metrics::v1::ExponentialHistogramDataPoint,
		aggregation_temporality: u64,
	},
	SummaryPoint {
		point: metrics::v1::SummaryDataPoint,
	},
}

impl Metric {
	fn from_otlp_metric(m: &metrics::v1::Metric) -> Vec<Self> {
		let mut v = Vec::new();
		match &m.data {
			Some(metrics::v1::metric::Data::Gauge(x)) => {
				for point in &x.data_points {
					v.push(Self::GaugePoint {
						point: point.clone(),
					});
				}
			}
			Some(metrics::v1::metric::Data::Sum(x)) => {
				let aggregation_temporality = x.aggregation_temporality as u64;
				let is_monotonic = x.is_monotonic;
				for point in &x.data_points {
					v.push(Self::SumPoint {
						point: point.clone(),
						aggregation_temporality,
						is_monotonic,
					});
				}
			}
			Some(metrics::v1::metric::Data::Histogram(x)) => {
				let aggregation_temporality = x.aggregation_temporality as u64;
				for point in &x.data_points {
					v.push(Self::HistogramPoint {
						point: point.clone(),
						aggregation_temporality,
					});
				}
			}
			Some(metrics::v1::metric::Data::ExponentialHistogram(x)) => {
				let aggregation_temporality = x.aggregation_temporality as u64;
				for point in &x.data_points {
					v.push(Self::ExponentialHistogramPoint {
						point: point.clone(),
						aggregation_temporality,
					});
				}
			}
			Some(metrics::v1::metric::Data::Summary(x)) => {
				for point in &x.data_points {
					v.push(Self::SummaryPoint {
						point: point.clone(),
					});
				}
			}
			_ => unimplemented!(),
		}
		v
	}

	fn timestamp(&self) -> u64 {
		match self {
			Metric::GaugePoint { point } => point.time_unix_nano / 1000,
			Metric::SumPoint { point, .. } => point.time_unix_nano / 1000,
			Metric::HistogramPoint { point, .. } => point.time_unix_nano / 1000,
			Metric::ExponentialHistogramPoint { point, .. } => {
				point.time_unix_nano / 1000
			}
			Metric::SummaryPoint { point, .. } => point.time_unix_nano / 1000,
		}
	}
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Resource {
	pub resource: Option<resource::v1::Resource>,
	pub schema_url: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InstrumentationScope {
	pub scope: Option<common::v1::InstrumentationScope>,
	pub schema_url: String,
}

pub trait FlattenOtlp {
	fn flatten_into(self, data: &mut OtlpDataset);
}

impl FlattenOtlp for trace::v1::ResourceSpans {
	fn flatten_into(self, vec: &mut OtlpDataset) {
		let resource = Resource {
			resource: self.resource,
			schema_url: self.schema_url,
		};
		for scope_span in self.scope_spans {
			let scope = InstrumentationScope {
				scope: scope_span.scope,
				schema_url: scope_span.schema_url,
			};
			for span in scope_span.spans {
				let timestamp_us = span.start_time_unix_nano / 1_000;
				let item = OtlpData {
					resource: resource.clone(),
					scope: scope.clone(),
					data: OtlpType::Span(span),
					timestamp_us,
				};
				vec.push(item);
			}
		}
	}
}

impl FlattenOtlp for logs::v1::ResourceLogs {
	fn flatten_into(self, vec: &mut OtlpDataset) {
		let resource = Resource {
			resource: self.resource,
			schema_url: self.schema_url,
		};
		for scope_log in self.scope_logs {
			let scope = InstrumentationScope {
				scope: scope_log.scope,
				schema_url: scope_log.schema_url,
			};
			for log_record in scope_log.log_records {
				let timestamp_us = log_record.time_unix_nano / 1000;
				let item = OtlpData {
					resource: resource.clone(),
					scope: scope.clone(),
					data: OtlpType::LogRecord(log_record),
					timestamp_us,
				};
				vec.push(item);
			}
		}
	}
}

impl FlattenOtlp for metrics::v1::ResourceMetrics {
	fn flatten_into(self, vec: &mut OtlpDataset) {
		let resource = Resource {
			resource: self.resource,
			schema_url: self.schema_url,
		};
		for scope_metric in self.scope_metrics {
			let scope = InstrumentationScope {
				scope: scope_metric.scope,
				schema_url: scope_metric.schema_url,
			};
			for metric in scope_metric.metrics {
				for metric in Metric::from_otlp_metric(&metric) {
					let timestamp_us = metric.timestamp();
					let item = OtlpData {
						resource: resource.clone(),
						scope: scope.clone(),
						data: OtlpType::Metric(metric),
						timestamp_us,
					};
					vec.push(item);
				}
			}
		}
	}
}

impl FlattenOtlp for collector::trace::v1::ExportTraceServiceRequest {
	fn flatten_into(self, vec: &mut OtlpDataset) {
		for resource_span in self.resource_spans {
			resource_span.flatten_into(vec);
		}
	}
}

impl FlattenOtlp for collector::logs::v1::ExportLogsServiceRequest {
	fn flatten_into(self, vec: &mut OtlpDataset) {
		for resource_log in self.resource_logs {
			resource_log.flatten_into(vec);
		}
	}
}

impl FlattenOtlp for collector::metrics::v1::ExportMetricsServiceRequest {
	fn flatten_into(self, vec: &mut OtlpDataset) {
		for resource_metric in self.resource_metrics {
			resource_metric.flatten_into(vec);
		}
	}
}
