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
    sha256 = "sha256-MwhWu4mK0UoZM887fXeaPyNbRmP3Q4Ddq3f8224TELg=";
  }];

  zstdStatic = final.callPackage "${nixpkgs}/pkgs/tools/compression/zstd" {
    static = true;
    buildContrib = false;
    doCheck = false;
  };

  # use the ancient version 0.29 of wasmtime because Scylla does not
  # build with newer ones
  wasmtime = final.callPackage ./pkg/upstreamable/wasmtime { };

  scylla-driver = final.callPackage ./pkg/upstreamable/python-driver { };
}
