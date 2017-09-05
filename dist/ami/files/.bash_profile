# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

. /usr/lib/scylla/scylla_lib.sh

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
echo 'By default, Scylla sends certain information about this node to a data collection server. For information, see http://www.scylladb.com/privacy/'
echo

SETUP=
if [ "$ID" != "ubuntu" ]; then
	SETUP=`systemctl is-active scylla-ami-setup`
fi
if [ "$SETUP" == "activating" ]; then
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
elif [ "$SETUP" == "failed" ]; then
	tput setaf 1
	tput bold
	echo "    AMI initial configuration failed!"
	tput sgr0
	echo
	echo "To see status, run "
	echo " 'systemctl status scylla-ami-setup'"
	echo
else
	if [ "$ID" != "ubuntu" ]; then
		SCYLLA=`systemctl is-active scylla-server`
	else
		if [ "`initctl status scylla-server|grep "running, process"`" != "" ]; then
			SCYLLA="active"
		else
			SCYLLA="failed"
		fi
	fi
	if [ "$SCYLLA" == "activating" ]; then
		tput setaf 4
		tput bold
		echo "    ScyllaDB is starting..."
		tput sgr0
		echo
		echo "Please wait for start. To see status, run "
		echo " 'systemctl status scylla-server'"
		echo
	elif [ "$SCYLLA" == "active" ]; then
		tput setaf 4
		tput bold
		echo "    ScyllaDB is active."
		tput sgr0
		echo
		echo "$ nodetool status"
		echo
		nodetool status
	else
		if [ `ec2_is_supported_instance_type` -eq 0 ]; then
			TYPE=`curl -s http://169.254.169.254/latest/meta-data/instance-type`
			tput setaf 1
			tput bold
			echo "    $TYPE is not supported instance type!"
			tput sgr0
			echo -n "To continue startup ScyllaDB on this instance, run 'sudo scylla_io_setup' "
			if [ "$ID" = "ubuntu" ]; then
				echo "then 'initctl start scylla-server'."
			else
				echo "then 'systemctl start scylla-server'."
			fi
			echo "For a list of optimized instance types and more EC2 instructions see http://www.scylladb.com/doc/getting-started-amazon/"
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
fi
echo -n "    "
/usr/lib/scylla/scylla_ec2_check
if [ $? -eq 0 ]; then
    echo
fi
