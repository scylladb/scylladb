{
  description = "Monstrously Fast + Scalable NoSQL";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }: {
    overlays.default = import ./dist/nix/overlay.nix nixpkgs;

    lib = {
      _attrs = system: let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlays.default ];
        };

        repl = pkgs.writeText "repl" ''
          let
            self = builtins.getFlake (toString ${self.outPath});
            attrs = self.lib._attrs "${system}";
          in {
            inherit self;
            inherit (attrs) pkgs;
          }
        '';

        args = {
          flake = true;
          srcPath = "${self}";
          inherit pkgs repl;
        };

        package = import ./default.nixpkgs args;
        devShell = import ./shell.nix args;
      in {
        inherit pkgs args package devShell;
      };
    };
  }
  // (flake-utils.lib.eachDefaultSystem (system: let
    packageName = "scylla";
    attrs = self.lib._attrs system;
  in {
    packages.${packageName} = attrs.package;
    defaultPackage = self.packages.${system}.${packageName};

    inherit (attrs) devShell;
  }));
}
