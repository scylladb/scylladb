# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

. /usr/lib/scylla/scylla_lib.sh

PATH=$PATH:$HOME/.local/bin:$HOME/bin

export PATH

get_en_interface_type() {
	TYPE=`curl -s http://169.254.169.254/latest/meta-data/instance-type|cut -d . -f 1`
	SUBTYPE=`curl -s http://169.254.169.254/latest/meta-data/instance-type|cut -d . -f 2`
	case $TYPE in
		"c3"|"c4"|"d2"|"i2"|"r3") echo -n "ixgbevf";;
		"i3"|"p2"|"r4"|"x1") echo -n "ena";;
		"m4")
			if [ "$SUBTYPE" = "16xlarge" ]; then
				echo -n "ena"
			else
				echo -n "ixgbevf"
			fi;;
	esac
}

is_vpc_enabled() {
	MAC=`cat /sys/class/net/eth0/address`
	VPC_AVAIL=`curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/|grep vpc-id`
	[ -n "$VPC_AVAIL" ]
}

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
TYPE=`curl -s http://169.254.169.254/latest/meta-data/instance-type`
EN=`get_en_interface_type`
DRIVER=`ethtool -i eth0|awk '/^driver:/ {print $2}'`
if [ "$EN" = "" ]; then
	tput setaf 1
	tput bold
	echo "    $TYPE doesn't support enahanced networking!"
	tput sgr0
	echo "To enable enhanced networking, please use the instance type which supports it."
	echo "More documentation available at: "
	echo "http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/enhanced-networking.html#enabling_enhanced_networking"
elif ! is_vpc_enabled; then
	tput setaf 1
	tput bold
	echo "    VPC is not enabled!"
	tput sgr0
	echo "To enable enhanced networking, please enable VPC."
elif [ "$DRIVER" != "$EN" ]; then
	tput setaf 1
	tput bold
	echo "    Enhanced networking is disabled!"
	tput sgr0
	echo "More documentation available at: "
	echo "http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/enhanced-networking.html"
fi
