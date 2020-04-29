Name: %{name}
Version: %{version}
Release: %{release}
Summary: A standalone python3 interpreter that can be moved around different Linux machines
AutoReqProv: no
Provides: %{name}

License: Python
Source0: %{reloc_pkg}

%global __brp_python_bytecompile %{nil}
%global __brp_mangle_shebangs %{nil}
%global __brp_ldconfig %{nil}
%global __brp_strip %{nil}
%global __brp_strip_comment_note %{nil}
%global __brp_strip_static_archive %{nil}

%description
This is a self-contained python interpreter that can be moved around
different Linux machines as long as they run a new enough kernel (where
new enough is defined by whichever Python module uses any kernel
functionality). All shared libraries needed for the interpreter to
operate are shipped with it.

%prep
%setup -n scylla-python3

%install
./install.sh --root "$RPM_BUILD_ROOT"

%files
%dir %{target}
%{target}/*

%changelog

