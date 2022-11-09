nixpkgs:
final: prev:

let
  patched = pkg: patches:
    pkg.overrideAttrs (old: {
      patches = (old.patches or []) ++ (map final.fetchurl patches);
    });
in {
  gdbWithGreenThreadSupport = patched prev.gdb [{
    url = "https://github.com/cmm/gnu-binutils/commit/1c52ca4b27e93e1684c68eeaee44ca3e36648410.patch";
    sha256 = "sha256-3s3KvN70dHMdr7Sx1dtzbZ8S+MynPTN7yCocoGlea2Y=";
  }];

  zstdStatic = final.callPackage "${nixpkgs}/pkgs/tools/compression/zstd" {
    static = true;
    buildContrib = false;
    doCheck = false;
  };

  cxxbridge = final.callPackage ./pkg/upstreamable/cxxbridge { };
  wasmtime = final.callPackage ./pkg/upstreamable/wasmtime { };

  scylla-driver = final.callPackage ./pkg/upstreamable/python-driver { };
}
