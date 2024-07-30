{ pkgs ? import <nixpkgs> {}}:
pkgs.stdenv.mkDerivation {
  name = "perf2perfetto";
  nativeBuildInputs = with pkgs; [
    llvmPackages.bintools
    llvmPackages.libclang
    cargo
    rustc
    rustPlatform.bindgenHook
  ];
  buildInputs = with pkgs; [];
}
