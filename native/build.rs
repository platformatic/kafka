fn main() {
  println!("cargo:rerun-if-changed=src/includes.h");

  let bindings = bindgen::Builder::default()
    .header("src/includes.h")
    .allowlist_var("KRB5_.*")
    .allowlist_var("GSS_.*")
    .generate()
    .expect("Unable to generate bindings");

  let bindings_path = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("bindings.rs");
  bindings
    .write_to_file(&bindings_path)
    .expect("Couldn't write bindings!");

  napi_build::setup();
}
