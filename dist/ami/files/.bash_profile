# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/.local/bin:$HOME/bin

export PATH

echo
echo '   _____            _ _       _____  ____  '
echo '  / ____|          | | |     |  __ \|  _ \ '
echo ' | (___   ___ _   _| | | __ _| |  | | |_) |'
echo '  \___ \ / __| | | | | |/ _` | |  | |  _ < '
echo '  ____) | (__| |_| | | | (_| | |__| | |_) |'
echo ' |_____/ \___|\__, |_|_|\__,_|_____/|____/ '
echo '               __/ |                       '
echo '              |___/                        '
echo ''
echo ''
echo 'Nodetool:'
echo '	nodetool help'
echo 'CQL Shell:'
echo '	cqlsh'
echo 'More documentation available at: '
echo '	http://www.scylladb.com/doc/'
echo

. /etc/os-release
SETUP=0
if [ "$ID" != "ubuntu" ]; then
	if [ "`systemctl status scylla-ami-setup|grep Active|grep exited`" = "" ]; then
		SETUP=1
	fi
fi
if [ $SETUP -eq 1 ]; then
	tput setaf 4
	tput bold
	echo "    Constructing RAID volume..."
	tput sgr0
	echo
	echo "Please wait for setup. To see status, run "
	echo " 'systemctl status scylla-ami-setup'"
	echo
	echo "After setup finished, scylla-server service will launch."
	echo "To see status of scylla-server, run "
	echo " 'systemctl status scylla-server'"
	echo
else
	if [ "$ID" = "ubuntu" ]; then
		if [ "`initctl status scylla-server|grep "running, process"`" != "" ]; then
			STARTED=1
		else
			STARTED=0
		fi
	else
		if [ "`systemctl is-active scylla-server`" = "active" ]; then
			STARTED=1
		else
			STARTED=0
		fi
	fi
	if [ $STARTED -eq 1 ]; then
		tput setaf 4
		tput bold
		echo "    ScyllaDB is active."
		tput sgr0
		echo
	else
		tput setaf 1
		tput bold
		echo "    ScyllaDB is not started!"
		tput sgr0
		echo "Please wait for startup. To see status of ScyllaDB, run "
		if [ "$ID" = "ubuntu" ]; then
			echo " 'initctl status scylla-server'"
			echo "and"
			echo " 'sudo cat /var/log/upstart/scylla-server.log'"
			echo
		else
			echo " 'systemctl status scylla-server'"
			echo
		fi
	fi
fi
