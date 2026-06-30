Name:           %{product}-cqlsh
Version:        %{version}
Release:        %{release}%{?dist}
Summary:        cqlsh is a Rust-based command-line client for running CQL commands on a cassandra cluster.
Group:          Applications/Databases
Obsoletes:      %{product}-tools < 5.2
Provides:       scylla-enterprise-cqlsh = %{version}-%{release}
Obsoletes:      scylla-enterprise-cqlsh < 2025.1.0

License:        MIT
URL:            http://www.scylladb.com/
Source0:        %{reloc_pkg}
AutoReqProv:    no
Conflicts:      cassandra

%global debug_package %{nil}

%global __brp_python_bytecompile %{nil}
%global __brp_mangle_shebangs %{nil}
%global __brp_ldconfig %{nil}
%global __brp_strip %{nil}
%global __brp_strip_comment_note %{nil}
%global __brp_strip_static_archive %{nil}


%description
cqlsh is a Rust-based command-line client for running CQL commands on a cassandra cluster.

%prep
%setup -q -n scylla-cqlsh


%build

%install
rm -rf $RPM_BUILD_ROOT
./install.sh --root "$RPM_BUILD_ROOT"

%files
%{_bindir}/cqlsh
%{_bindir}/cqlsh-rs
/opt/scylladb/share/cassandra/bin/*


%changelog
* Wed Sep 14 2022 Israel Fruchter <fruch@scylladb.com>
- initial release
