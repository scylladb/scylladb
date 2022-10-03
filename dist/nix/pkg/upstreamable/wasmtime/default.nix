{ pkgs
}:

let
  inherit (pkgs)
    cmake
    fetchFromGitHub
    python3
    rustPlatform
  ;

  llvm = pkgs.llvmPackages_latest;
  clang = llvm.clang;

  pname = "wasmtime";
  version = "0.29.0";

in rustPlatform.buildRustPackage {
  inherit pname version;
  src = fetchFromGitHub {
    owner = "bytecodealliance";
    repo = pname;
    rev = "v${version}";
    sha256 = "sha256-qmME9zI2vSFQlJWhve7dqAPh8O/6WclWTwO9Pcdvoj8=";
    fetchSubmodules = true;
  };

  cargoSha256 = "sha256-xtx1iCtZ9rRJec6Q7ywBM3ov9z0YJMcxz7K59Ox6DKk=";

  nativeBuildInputs = [ python3 cmake clang ];
  buildInputs = [ llvm.libclang ];
  LIBCLANG_PATH = "${llvm.libclang.lib}/lib";

  cargoBuildFlags = [ "--package wasmtime-c-api" ];

  # cargo does not install the C(++) headers
  postInstall = ''
    install -d -m744 $out/include/wasmtime
    install -m644 $src/crates/c-api/include/*.h $out/include
    install -m644 $src/crates/c-api/include/wasmtime/*.h $out/include/wasmtime
    install -m644 $src/crates/c-api/wasm-c-api/include/* $out/include
  '';

  # Scylla has to use this version (later ones change APIs), and it
  # happens not to pass its own tests, so:
  doCheck = false;
}
