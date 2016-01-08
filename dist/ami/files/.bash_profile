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
echo '	nodetool --help'
echo 'CQL Shell:'
echo '	cqlsh'
echo 'More documentation available at: '
echo '	http://www.scylladb.com/doc/'
echo

if [ "`systemctl is-active scylla-server`" = "active" ]; then
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
	echo " 'systemctl status scylla-server'"
	echo
fi
