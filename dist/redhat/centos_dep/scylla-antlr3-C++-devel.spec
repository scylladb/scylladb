%global antlr_version 3.5.2

Name:		scylla-antlr3-C++-devel
Version:        %{antlr_version}
Release:	1%{?dist}
Summary:        C++ runtime support for ANTLR-generated parsers

License:        BSD
URL:            http://www.antlr3.org/
Source0:        https://github.com/antlr/antlr3/archive/%{antlr_version}.tar.gz
Requires:	scylla-env
%define _prefix /opt/scylladb

%description
C++ runtime support for ANTLR-generated parsers.

%prep
%setup -q -n antlr3-%{antlr_version}

%build


%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{_includedir}
install -m644 runtime/Cpp/include/* $RPM_BUILD_ROOT%{_includedir}

%files
%{_includedir}/*.hpp
%{_includedir}/*.inl


%changelog
