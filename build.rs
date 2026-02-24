fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "protos/";

    println!("cargo:rerun-if-changed={}", proto_root);

    let all_proto_files: Vec<_> = std::fs::read_dir("protos")?
        .filter_map(|e| {
            let path = e.ok()?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("proto") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .field_attribute(
            "similarityhit.QdrantPayload.market_subcategory",
            "#[serde(default)]",
        )
        .field_attribute("similarityhit.QdrantPayload.end_date", "#[serde(default)]")
        .compile_protos(&all_proto_files, &[proto_root.into()])?;

    Ok(())
}
