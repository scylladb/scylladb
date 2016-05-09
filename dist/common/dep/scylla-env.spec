Name:		scylla-env
Version:	1.0
Release:	1%{?dist}
Summary:	Scylla is a highly scalable, eventually consistent, distributed, partitioned row DB.

Group:		Applications/Databases
License:	AGPLv3
URL:		http://www.scylladb.com/
Source0:	scylla-env-1.0.tar
BuildArch:	noarch

%description


%prep
%setup -q


%build


%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/profile.d
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/ld.so.conf.d
install -m 644 profile.d/* $RPM_BUILD_ROOT%{_sysconfdir}/profile.d
install -m 644 ld.so.conf.d/* $RPM_BUILD_ROOT%{_sysconfdir}/ld.so.conf.d

%post
%{_sbindir}/ldconfig

%files
%doc
%{_sysconfdir}/profile.d/scylla.sh
%{_sysconfdir}/profile.d/scylla.csh
%{_sysconfdir}/ld.so.conf.d/scylla.x86_64.conf


%changelog

