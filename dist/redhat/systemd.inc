%define systemd_daemon_reload \
if [ -x /usr/bin/systemctl ]; then \
    /usr/bin/systemctl daemon-reload ||: \
fi \
%{nil}

%undefine systemd_post
%define systemd_post() %{expand:%systemd_daemon_reload}

%undefine systemd_preun
%define systemd_preun() \
if [ $1 -eq 0 ] && [ -x /usr/bin/systemctl ]; then \
    /usr/bin/systemctl disable %1 ||: \
    /usr/bin/systemctl stop %1 ||: \
fi \
%{nil}

%undefine systemd_postun
%define systemd_postun() %{expand:%systemd_daemon_reload}
