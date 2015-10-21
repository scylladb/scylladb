%global antlr_version 3.5.2

Name:		scylla-antlr3-tool
Version:	%{antlr_version}
Release:	1%{?dist}
Summary:	ANother Tool for Language Recognition

License:	BSD
URL:            http://www.antlr3.org/
Source0:	%{name}-%{version}.tar.xz

BuildArch:	noarch
Requires:	java-1.7.0-openjdk
Requires:	scylla-env
%define _prefix /opt/scylladb

%description
ANother Tool for Language Recognition, is a language tool
that provides a framework for constructing recognizers,
interpreters, compilers, and translators from grammatical
descriptions containing actions in a variety of target languages.

%prep
%setup -q


%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{_bindir}
mkdir -p $RPM_BUILD_ROOT%{_prefix}/lib/java
install -m755 antlr3 $RPM_BUILD_ROOT%{_bindir}
install -m644 antlr-3.5.2-complete-no-st3.jar $RPM_BUILD_ROOT%{_prefix}/lib/java/antlr3.jar


%files
%defattr(-,root,root)

%{_bindir}/antlr3
%{_prefix}/lib/java/antlr3.jar


%changelog
* Sun Aug 30 2015 Takuya ASADA <syuu@cloudius-systems.com>
- inital version of antlr3-tool.spec
