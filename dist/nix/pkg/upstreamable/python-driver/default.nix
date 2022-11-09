{ stdenv
, lib
, fetchFromGitHub
, python3Packages
, libev
}:

let
  version = "3.25.4-scylla";
in python3Packages.buildPythonPackage {
  pname = "scylla-driver";
  inherit version;

  # pypi tarball doesn't include tests
  src = fetchFromGitHub {
    owner = "scylladb";
    repo = "python-driver";
    rev = version;
    sha256 = "sha256-LIPZ4sY/wrhmy+kpFUwBvgvbJoXanQLkzMd5Iv0X2mc=";
  };

  postPatch = ''
    substituteInPlace setup.py --replace 'geomet>=0.1,<0.3' 'geomet'
  '';

  nativeBuildInputs = with python3Packages; [ cython ];
  buildInputs = [ libev ];
  propagatedBuildInputs = with python3Packages; [ six geomet ];

  doCheck = false;
}
