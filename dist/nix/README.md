# Nix development environment

The `*.nix` files at the project root and under `dist/nix` implement a
[Nix](https://nixos.org/guides/ad-hoc-developer-environments.html)-based
development environment for Scylla.

Note that there is presently no support for building installable
artifacts with `nix build`, or anything suitable for incorporation
into the `Nixpkgs` collection (that's "just" a matter of implementing
`installPhase` in `default.nix`).  This is just a development
environment that is predictable, is independent from the state of the
host distribution, and does not require entering a container.

`gdb` with green thread debugging support is included, plus other
assorted debugging tools.

Compilers are transparently wrapped to use `ccache` and `distcc`, if
you have those configured.

## Basic usage

If you have Nix flake support enabled (recommended!), enter the
environment using `nix develop .`.  Otherwise, use `nix-shell .`.

`$configPhase` will configure Scylla for building (it just invokes
`./configure.py --disable-dpdk`).  Then use `ninja` to build,
`test.py` to run unit tests, etc., as usual.

## Direnv (optional)

Using [direnv](https://direnv.net) is recommended to make life more convenient.  Create
`.envrc` at the project root with the contents:
```bash
nix_direnv_watch_file default.nix
nix_direnv_watch_file shell.nix
for f in $(find dist/nix/ -type f); do
    nix_direnv_watch_file $f
done
use flake
```

For non-flake usage, replace the last line with `use nix`.
