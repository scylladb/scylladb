{ pkgs
}:

let
  inherit (pkgs)
    fetchFromGitHub
    rustPlatform
  ;

  version = "1.0.68";
  lockFile = ./Cargo.lock;

in rustPlatform.buildRustPackage {
  pname = "cxxbridge-cmd";
  inherit version;

  src = fetchFromGitHub {
    owner = "dtolnay";
    repo = "cxx";
    rev = "${version}";
    sha256 = "sha256-DdcbPcxTGJ5rJUJxQR3YBHbe9g3JjgbP/htJqWerRlI=";
  };

  cargoLock = { inherit lockFile; };
  postPatch = "cp ${lockFile} Cargo.lock";

  cargoBuildFlags = [ "--package cxxbridge-cmd" ];
}
