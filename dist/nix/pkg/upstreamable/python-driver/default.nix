{ stdenv
, lib
, fetchFromGitHub
, python3Packages
, libev
}:

let
  version = "3.25.10-scylla";
in python3Packages.buildPythonPackage {
  pname = "scylla-driver";
  inherit version;

  # pypi tarball doesn't include tests
  src = fetchFromGitHub {
    owner = "scylladb";
    repo = "python-driver";
    rev = version;
    sha256 = "sha256-ib1XZPLcg5lCMfbUhDwjB968HYtGjt559JX5fxDADQc=";
  };

  postPatch = ''
    substituteInPlace setup.py --replace 'geomet>=0.1,<0.3' 'geomet'
    substituteInPlace setup.py --replace 'cython>=0.20,<0.30' 'cython'
  '';

  nativeBuildInputs = with python3Packages; [ cython pyyaml ];
  buildInputs = [ libev ];
  propagatedBuildInputs = with python3Packages; [ six geomet ];

  doCheck = false;
}
