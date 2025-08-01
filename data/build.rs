use std::fs::create_dir_all;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let derive_serde = "#[derive(serde::Serialize, serde::Deserialize)]";

	let protos = &[
		"pb/opentelemetry/proto/collector/logs/v1/logs_service.proto",
		"pb/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
		"pb/opentelemetry/proto/collector/trace/v1/trace_service.proto",
	];

	create_dir_all("pb-rs").unwrap();

	tonic_build::configure()
		.out_dir("pb-rs")
		.protoc_arg("--experimental_allow_proto3_optional")
		.message_attribute(".", derive_serde)
		.enum_attribute(".", derive_serde)
		.compile(protos, &["pb"])?;

	Ok(())
}
