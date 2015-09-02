Name:           scylla-server
Version:        0.00
Release:        1%{?dist}
Summary:        Scylla is a highly scalable, eventually consistent, distributed, partitioned row DB.
Group:          Applications/Databases

License:        Proprietary
URL:            http://www.seastar-project.org/
Source0:        %{name}-%{version}.tar

BuildRequires:  libaio-devel boost-devel libstdc++-devel cryptopp-devel xen-devel hwloc-devel numactl-devel libpciaccess-devel libxml2-devel zlib-devel thrift-devel yaml-cpp-devel lz4-devel snappy-devel jsoncpp-devel systemd-devel xz-devel openssl-devel libcap-devel libselinux-devel libgcrypt-devel libgpg-error-devel elfutils-devel krb5-devel libcom_err-devel libattr-devel pcre-devel elfutils-libelf-devel bzip2-devel keyutils-libs-devel ninja-build ragel antlr3-tool antlr3-C++-devel libasan libubsan gcc-c++ make
Requires:       libaio boost-program-options boost-system libstdc++ boost-thread cryptopp xen-libs hwloc-libs numactl-libs libpciaccess libxml2 zlib thrift yaml-cpp lz4 snappy jsoncpp boost-filesystem systemd-libs xz-libs openssl-libs libcap libselinux libgcrypt libgpg-error elfutils-libs krb5-libs libcom_err libattr pcre elfutils-libelf bzip2-libs keyutils-libs

# TODO: create our own bridge device for virtio
Requires:       libvirt-daemon

Conflicts:       cassandra21
Provides:       cassandra21

%description

%prep
%setup -q

%build
./configure.py --with scylla --disable-xen --enable-dpdk --mode=release
ninja-build %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{_bindir}
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/sysconfig/
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/security/limits.d/
mkdir -p $RPM_BUILD_ROOT%{_docdir}/scylla/
mkdir -p $RPM_BUILD_ROOT%{_unitdir}
mkdir -p $RPM_BUILD_ROOT%{_prefix}/lib/scylla/

install -m644 dist/redhat/sysconfig/scylla-server $RPM_BUILD_ROOT%{_sysconfdir}/sysconfig/
install -m644 dist/redhat/limits.d/scylla.conf $RPM_BUILD_ROOT%{_sysconfdir}/security/limits.d/
install -m644 dist/redhat/systemd/scylla-server.service $RPM_BUILD_ROOT%{_unitdir}/
install -m755 dist/redhat/scripts/* $RPM_BUILD_ROOT%{_prefix}/lib/scylla/
install -m755 seastar/dpdk/tools/dpdk_nic_bind.py $RPM_BUILD_ROOT%{_prefix}/lib/scylla/
install -m755 build/release/scylla $RPM_BUILD_ROOT%{_bindir}
install -d -m755 $RPM_BUILD_ROOT%{_docdir}/scylla
install -m644 README.md $RPM_BUILD_ROOT%{_docdir}/scylla/
install -m644 README-DPDK.md $RPM_BUILD_ROOT%{_docdir}/scylla/
install -m644 NOTICE.txt $RPM_BUILD_ROOT%{_docdir}/scylla/
install -m644 ORIGIN $RPM_BUILD_ROOT%{_docdir}/scylla/
install -d -m755 $RPM_BUILD_ROOT%{_docdir}/scylla/licenses/
install -m644 licenses/* $RPM_BUILD_ROOT%{_docdir}/scylla/licenses/
install -d -m755 $RPM_BUILD_ROOT%{_sharedstatedir}/scylla/
install -d -m755 $RPM_BUILD_ROOT%{_sharedstatedir}/scylla/data
install -d -m755 $RPM_BUILD_ROOT%{_sharedstatedir}/scylla/commitlog
install -d -m755 $RPM_BUILD_ROOT%{_sharedstatedir}/scylla/conf
install -m644 conf/scylla.yaml $RPM_BUILD_ROOT%{_sharedstatedir}/scylla/conf/

%pre
/usr/sbin/groupadd scylla 2> /dev/null || :
/usr/sbin/useradd -g scylla -s /sbin/nologin -r -d %{_sharedstatedir}/scylla scylla 2> /dev/null || :

%post
%systemd_post scylla-server.service

%preun
%systemd_preun scylla-server.service

%postun
%systemd_postun

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

%{_sysconfdir}/sysconfig/scylla-server
%{_sysconfdir}/security/limits.d/scylla.conf
%{_docdir}/scylla/README.md
%{_docdir}/scylla/README-DPDK.md
%{_docdir}/scylla/NOTICE.txt
%{_docdir}/scylla/ORIGIN
%{_docdir}/scylla/licenses/
%{_unitdir}/scylla-server.service
%{_bindir}/scylla
%{_prefix}/lib/scylla/scylla_prepare
%{_prefix}/lib/scylla/scylla_run
%{_prefix}/lib/scylla/scylla_stop
%{_prefix}/lib/scylla/dpdk_nic_bind.py
%{_prefix}/lib/scylla/dpdk_nic_bind.pyc
%{_prefix}/lib/scylla/dpdk_nic_bind.pyo
%attr(0700,scylla,scylla) %dir %{_sharedstatedir}/scylla/
%attr(0700,scylla,scylla) %dir %{_sharedstatedir}/scylla/data
%attr(0700,scylla,scylla) %dir %{_sharedstatedir}/scylla/commitlog
%{_sharedstatedir}/scylla/conf/

%changelog
* Tue Jul 21 2015 Takuya ASADA <syuu@cloudius-systems.com>
- inital version of scylla.spec
