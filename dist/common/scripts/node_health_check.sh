#!/bin/bash

##Variables##
REPORT="./`hostname -i`-health-check-report.txt"
OUTPUT_PATH="./output_files"
OUTPUT_PATH1="$OUTPUT_PATH/system_checks"
OUTPUT_PATH2="$OUTPUT_PATH/scylladb_checks"
OUTPUT_PATH3="$OUTPUT_PATH/nodetool_commands"
OUTPUT_PATH4="$OUTPUT_PATH/data_model"
OUTPUT_PATH5="$OUTPUT_PATH/network_checks"
IS_FEDORA="0"
IS_DEBIAN="0"


##Print flags usage##
print_dm=NO
print_net=NO
print_cfstats=NO

while getopts 'hdncaz:' opt; do
	case $opt in
		h)
                        echo ""
                        echo "This script performs system review and generates health check report based on"
                        echo "the configuration data (hardware, OS, Scylla SW, etc.) collected from the node."
                        echo ""
                        echo "Note: the output of these sepcific sections is collected, but not printed in the report."
                        echo "If you wish to have them printed, please supply the relevant flag/s."
                        echo ""
			echo "Usage:"
			echo "-c   Print cfstats"
                        echo "-d   Print Data Model"
			echo "-n   Print Network Info"
			echo "-a   Print All"
			echo ""
			exit 2
			;;
                d) print_dm=YES ;;
                n) print_net=YES ;;
                c) print_cfstats=YES ;;
                a) print_dm=YES
                   print_net=YES
                   print_cfstats=YES
		   ;;
	esac
done


##Check if server is Fedora/Debian release##
cat /etc/os-release | grep fedora &> /dev/null

if [ $? -ne 0 ]; then
	IS_FEDORA="1"
fi

cat /etc/os-release | grep debian &> /dev/null

if [ $? -ne 0 ]; then
	IS_DEBIAN="1"
fi


##Pass criteria for script execution##
#Check scylla service#
echo "--------------------------------------------------"
echo "Checking Scylla Service"
echo "--------------------------------------------------"
systemctl status scylla-server &> /dev/null

if [ $? -ne 0 ]; then
	echo "ERROR: Scylla is NOT Running - Exit"
	echo "--------------------------------------------------"
	exit 222
fi 

echo "Scylla Service: OK"
echo "--------------------------------------------------"

#Check JMX service#
echo "Checking JMX Service (Nodetool)"
echo "--------------------------------------------------"
nodetool status &> /dev/null

if [ $? -ne 0 ]; then
	echo "ERROR: JMX is NOT Running - Exit"
	echo "--------------------------------------------------"
	exit 222
fi 

echo "JMX Service (Nodetool): OK"
echo "--------------------------------------------------"


#Install 'net-tools' pkg, to be used for netstat command#
echo "Installing 'net-tools' Package (for 'netstat' command)"
echo "--------------------------------------------------"

if [ "${IS_FEDORA}" == "0" ]; then
	sudo yum install net-tools -y -q
fi

if [ "${IS_DEBIAN}" == "0" ]; then
	sudo apt-get update -qq
	sudo apt-get install net-tools -y -qq
fi


#Install 'lshw' pkg, for IO sched conf output#
echo "--------------------------------------------------"
echo "Installing 'lshw' Package (for IO sched conf output)"
echo "--------------------------------------------------"

if [ "${IS_FEDORA}" == "0" ]; then
   sudo yum install lshw -y -q
fi

if [ "${IS_DEBIAN}" == "0" ]; then
   sudo apt-get install lshw -y -qq
fi


#Create dir structure to save output_files#
echo "--------------------------------------------------"
echo "Creating Output Files Directory"
echo "--------------------------------------------------"
mkdir $OUTPUT_PATH
mkdir $OUTPUT_PATH1 $OUTPUT_PATH2 $OUTPUT_PATH3 $OUTPUT_PATH4 $OUTPUT_PATH5


##Output Collection##
#System Checks#
echo "Collecting System Info"
echo "--------------------------------------------------"
head -n6 /etc/os-release >> $OUTPUT_PATH1/os-release.txt
uname -r >> $OUTPUT_PATH1/kernel-release.txt
grep -c ^processor /proc/cpuinfo >> $OUTPUT_PATH1/cpu-count.txt
lscpu >> $OUTPUT_PATH1/cpu-info.txt
free -m >> $OUTPUT_PATH1/mem-info_MB.txt
vmstat -s -S M | awk '{$1=$1};1' >> $OUTPUT_PATH1/vmstat.txt
df -Th >> $OUTPUT_PATH1/capacity-info.txt
echo "" >> $OUTPUT_PATH1/capacity-info.txt
sudo du -sh /var/lib/scylla/* >> $OUTPUT_PATH1/capacity-info.txt
cat /proc/mdstat >> $OUTPUT_PATH1/raid-conf.txt
sudo lshw | for f in `sudo find /sys -name scheduler`; do echo -n "$f: "; cat  $f; done >> $OUTPUT_PATH1/io-sched-conf.txt
echo "" >> $OUTPUT_PATH1/io-sched-conf.txt
sudo lshw | for f in `sudo find /sys -name nomerges`; do echo -n "$f: "; cat  $f; done >> $OUTPUT_PATH1/io-sched-conf.txt


#ScyllaDB Checks#
echo "Collecting Scylla Info"
echo "--------------------------------------------------"

if [ "${IS_FEDORA}" == "0" ]; then
	rpm -qa | grep -i scylla >> $OUTPUT_PATH2/scylla-pkgs.txt
fi

if [ "${IS_DEBIAN}" == "0" ]; then
	dpkg -l | grep -i scylla >> $OUTPUT_PATH2/scylla-pkgs.txt
fi

curl -s -X GET "http://localhost:10000/storage_service/scylla_release_version" >> $OUTPUT_PATH2/scylla-version.txt && echo "" >> $OUTPUT_PATH2/scylla-version.txt
cat /etc/scylla/scylla.yaml | grep -v "#" | grep -v "^[[:space:]]*$" >> $OUTPUT_PATH2/scylla-yaml.txt

if [ "${IS_FEDORA}" == "0" ]; then
	cat /etc/sysconfig/scylla-server | grep -v "^[[:space:]]*$" >> $OUTPUT_PATH2/scylla-server.txt
fi

if [ "${IS_DEBIAN}" == "0" ]; then
	cat /etc/default/scylla-server | grep -v "^[[:space:]]*$" >> $OUTPUT_PATH2/scylla-server.txt
fi

cat /etc/scylla/cassandra-rackdc.properties | grep -v "#" |grep -v "^[[:space:]]*$" >> $OUTPUT_PATH2/multi-DC.txt
ls -ltrh /var/lib/scylla/coredump/ >> $OUTPUT_PATH2/coredump-folder.txt


#Scylla Logs#
echo "Collecting Logs"
echo "--------------------------------------------------"

if [ "${IS_FEDORA}" == "0" ]; then
	journalctl -t scylla >> $OUTPUT_PATH/scylla-logs.txt
fi

if [ "${IS_DEBIAN}" == "0" ]; then
	cat /var/log/syslog | grep -i scylla >> $OUTPUT_PATH/scylla-logs.txt
fi

gzip $OUTPUT_PATH/scylla-logs.txt


#Nodetool commands#
echo "Collecting Nodetool Commands Info"
echo "--------------------------------------------------"
nodetool status >> $OUTPUT_PATH3/nodetool-status.txt
nodetool info >> $OUTPUT_PATH3/nodetool-info.txt
nodetool netstats >> $OUTPUT_PATH3/nodetool-netstats.txt
nodetool gossipinfo >> $OUTPUT_PATH3/nodetool-gossipinfo.txt
nodetool proxyhistograms >> $OUTPUT_PATH3/nodetool-proxyhistograms.txt
nodetool cfstats -H | grep Keyspace -A 4 >> $OUTPUT_PATH3/nodetool-cfstats-keyspace.txt
nodetool cfstats -H | egrep 'Table:|SSTable count:|Compacted|tombstones' | awk '{$1=$1};1' | awk '{print; if (FNR % 7 == 0 ) printf "\n --";}' >> $OUTPUT_PATH3/nodetool-cfstats-table.txt
sed -i '1s/^/ --/' $OUTPUT_PATH3/nodetool-cfstats-table.txt
nodetool compactionstats >> $OUTPUT_PATH3/nodetool-compactionstats.txt
nodetool ring >> $OUTPUT_PATH3/nodetool-ring.txt
#not implemented: nodetool cfhistograms $KS $TN >> $OUTPUT_PATH3/nodetool-cfhistograms.txt#


#Data Model#
echo "Collecting Data Model Info"
echo "--------------------------------------------------"
cqlsh `hostname -i` -e "DESCRIBE SCHEMA" >> $OUTPUT_PATH4/describe-schema.txt
cqlsh `hostname -i` -e "DESCRIBE TABLES" >> $OUTPUT_PATH4/describe-tables.txt


#Network Checks#
echo "Collecting Network Info"
echo "--------------------------------------------------"
ifconfig -a >> $OUTPUT_PATH5/ifconfig.txt
for i in `ls -I lo /sys/class/net/`; do echo "--$i"; ethtool -i $i; echo ""; done >> $OUTPUT_PATH5/ethtool-NIC.txt
cat /proc/interrupts >> $OUTPUT_PATH5/proc-interrupts.txt
for i in `ls -I default_smp_affinity /proc/irq`; do echo -n "--$i:"; sudo cat /proc/irq/$i/smp_affinity; echo ""; done >> $OUTPUT_PATH5/irq-smp-affinity.txt
for i in `ls -I lo /sys/class/net/`; do echo "--$i"; cat /sys/class/net/$i/queues/rx-*/rps_cpus; echo ""; done >> $OUTPUT_PATH5/rps-conf.txt
for i in `ls -I lo /sys/class/net/`; do echo "--$i"; cat /sys/class/net/$i/queues/tx-*/xps_cpus; echo ""; done >> $OUTPUT_PATH5/xps-conf.txt
for i in `ls -I lo /sys/class/net/`; do echo "--$i"; cat /sys/class/net/$i/queues/rx-*/rps_flow_cnt; echo ""; done >> $OUTPUT_PATH5/rfs-conf.txt
ps -elf | grep irqbalance >> $OUTPUT_PATH5/irqbalance-conf.txt
sudo sysctl -a >> $OUTPUT_PATH5/sysctl.txt
sudo iptables -L >> $OUTPUT_PATH5/iptables.txt
netstat -an | grep tcp >> $OUTPUT_PATH5/netstat-tcp.txt


echo "Output Collection Completed Successfully"
echo "--------------------------------------------------"


##Generate Health Check Report##
echo "Generating Health Check Report"
echo "--------------------------------------------------"
echo "Print cfstats: ${print_cfstats}"
echo "Print Data Model: ${print_dm}"
echo "Print Network Info: ${print_net}"
echo "--------------------------------------------------"

echo "" >> $REPORT
echo "                    Health Check Report for `hostname -i`" >> $REPORT 
echo "" >> $REPORT
echo "" >> $REPORT


echo "PURPOSE" >> $REPORT
echo "=======" >> $REPORT
echo "" >> $REPORT
echo "This document first serves as a system review and health check report." >> $REPORT
echo "It is based on the configuration data (hardware, OS, Scylla SW, etc.) collected from the node." >> $REPORT
echo "Based on the review and analysis of the collected data, ScyllaDB can recommend on possible" >> $REPORT
echo "ways to better utilize the cluster, based on both experiance and best practices." >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT


echo "SYSTEM INFO" >> $REPORT
echo "===========" >> $REPORT
echo "" >> $REPORT

echo "Host Operating System" >> $REPORT
echo "---------------------" >> $REPORT
cat $OUTPUT_PATH1/os-release.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Kernel Release" >> $REPORT
echo "--------------" >> $REPORT
cat $OUTPUT_PATH1/kernel-release.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Number of CPUs and CPU Info" >> $REPORT
echo "---------------------------" >> $REPORT
cat $OUTPUT_PATH1/cpu-count.txt >> $REPORT
echo "" >> $REPORT
cat $OUTPUT_PATH1/cpu-info.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Memory Info in MB" >> $REPORT
echo "-----------------" >> $REPORT
cat $OUTPUT_PATH1/mem-info_MB.txt >> $REPORT
echo "" >> $REPORT
cat $OUTPUT_PATH1/vmstat.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Storage/Disk Info" >> $REPORT
echo "-----------------" >> $REPORT
cat $OUTPUT_PATH1/capacity-info.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "RAID Configuration" >> $REPORT
echo "------------------" >> $REPORT
cat $OUTPUT_PATH1/raid-conf.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "I/O Scheduler Configuration" >> $REPORT
echo "---------------------------" >> $REPORT
cat $OUTPUT_PATH1/io-sched-conf.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT


echo "ScyllaDB INFO" >> $REPORT
echo "=============" >> $REPORT
echo "" >> $REPORT

echo "SW Version (PKGs)" >> $REPORT
echo "-----------------" >> $REPORT
cat $OUTPUT_PATH2/scylla-version.txt >> $REPORT
echo "" >> $REPORT
cat $OUTPUT_PATH2/scylla-pkgs.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Configuration files" >> $REPORT
echo "-------------------" >> $REPORT
echo "## /etc/scylla/scylla.yaml ##" >> $REPORT
cat $OUTPUT_PATH2/scylla-yaml.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

if [ "${IS_FEDORA}" == "0" ]; then
   echo "## /etc/sysconfig/scylla-server ##" >> $REPORT
fi

if [ "${IS_DEBIAN}" == "0" ]; then
   echo "## /etc/default/scylla-server ##" >> $REPORT
fi

cat $OUTPUT_PATH2/scylla-server.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "## /etc/scylla/cassandra-rackdc.properties ##" >> $REPORT
cat $OUTPUT_PATH2/multi-DC.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Check for Coredumps" >> $REPORT
echo "-------------------" >> $REPORT
cat $OUTPUT_PATH2/coredump-folder.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Nodetool Status/Info/Gossip" >> $REPORT
echo "---------------------------" >> $REPORT
echo "## Nodetool Status ##" >> $REPORT
cat $OUTPUT_PATH3/nodetool-status.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "## Nodetool Info ##" >> $REPORT
cat $OUTPUT_PATH3/nodetool-info.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "## Nodetool GossipInfo ##" >> $REPORT
cat $OUTPUT_PATH3/nodetool-gossipinfo.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

if [ $print_dm == "YES" ]; then
        echo "Printing Data Model Info to Report"
        echo "--------------------------------------------------"

	echo "DATA MODEL INFO" >> $REPORT
	echo "===============" >> $REPORT
	echo "" >> $REPORT

	echo "Describe Schema" >> $REPORT
	echo "---------------" >> $REPORT
	cat $OUTPUT_PATH4/describe-schema.txt >> $REPORT
	echo "" >> $REPORT

	echo "Describe Tables" >> $REPORT
	echo "---------------" >> $REPORT
	cat $OUTPUT_PATH4/describe-tables.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT
fi


echo "PERFORMANCE and METRICS INFO" >> $REPORT
echo "============================" >> $REPORT
echo "" >> $REPORT

echo "Nodetool Proxyhistograms (RD/WR latency)" >> $REPORT
echo "----------------------------------------" >> $REPORT
cat $OUTPUT_PATH3/nodetool-proxyhistograms.txt >> $REPORT
echo "" >> $REPORT

echo "Nodetool netstats" >> $REPORT
echo "-----------------" >> $REPORT
cat $OUTPUT_PATH3/nodetool-netstats.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

if [ $print_cfstats == "YES" ]; then
        echo "Printing cfstats Output to Report"
        echo "--------------------------------------------------"

	echo "Nodetool cfstats" >> $REPORT
	echo "----------------" >> $REPORT
	echo "## Keyspace Info ##" >> $REPORT
	cat $OUTPUT_PATH3/nodetool-cfstats-keyspace.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT
	echo "## Tables Info ##" >> $REPORT
	cat $OUTPUT_PATH3/nodetool-cfstats-table.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT
fi

echo "Nodetool compactionstats" >> $REPORT
echo "------------------------" >> $REPORT
cat $OUTPUT_PATH3/nodetool-compactionstats.txt >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT


if [ $print_net == "YES" ]; then
	echo "Printing Network Info to Report"
	echo "--------------------------------------------------"
	echo "" >> $REPORT

	echo "NETWORK INFO" >> $REPORT
	echo "============" >> $REPORT
	echo "" >> $REPORT

	echo "ethtool per NIC" >> $REPORT
	echo "---------------" >> $REPORT
	cat $OUTPUT_PATH5/ethtool-NIC.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT

	echo "/proc/interrupts" >> $REPORT
	echo "----------------" >> $REPORT
	cat $OUTPUT_PATH5/proc-interrupts.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT

	echo "IRQ smp affinity" >> $REPORT
	echo "----------------" >> $REPORT
	cat $OUTPUT_PATH5/irq-smp-affinity.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT

	echo "sysctl -a" >> $REPORT
	echo "---------" >> $REPORT
	cat $OUTPUT_PATH5/sysctl.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT

	echo "iptables -L" >> $REPORT
	echo "-----------" >> $REPORT
	cat $OUTPUT_PATH5/iptables.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT

	echo "netstat -an | grep tcp" >> $REPORT
	echo "----------------------" >> $REPORT
	cat $OUTPUT_PATH5/netstat-tcp.txt >> $REPORT
	echo "" >> $REPORT
	echo "" >> $REPORT
fi


echo "MANUAL CHECK LIST" >> $REPORT
echo "=================" >> $REPORT
echo "" >> $REPORT

echo "Security Review" >> $REPORT
echo "---------------" >> $REPORT
echo "Check the following links:"  >> $REPORT
echo "- http://www.scylladb.com/2017/02/06/making-sure-your-scylla-cluster-is-secure/" >> $REPORT
echo "- http://docs.scylladb.com/tls-ssl/" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Backup / Restore Review" >> $REPORT
echo "-----------------------" >> $REPORT
echo "Check the following links:"  >> $REPORT
echo "- http://docs.scylladb.com/procedures/backup/" >> $REPORT
echo "- http://docs.scylladb.com/procedures/restore/" >> $REPORT
echo "- http://docs.scylladb.com/procedures/delete_snapshot/" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Repair Verification" >> $REPORT
echo "-------------------" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Single node / DC Failure Test" >> $REPORT
echo "-----------------------------" >> $REPORT
echo "Check the following links:" >> $REPORT
echo "- http://docs.scylladb.com/procedures/replace_dead_node/" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT

echo "Other" >> $REPORT
echo "-----" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT


echo "Signatures" >> $REPORT
echo "==========" >> $REPORT
date "+DATE: %m/%d/%y" >> $REPORT
echo "" >> $REPORT
echo "" >> $REPORT
echo "Scylla:________________    Customer:________________" >> $REPORT
echo "" >> $REPORT

echo "Archiving Output Files"
echo "--------------------------------------------------"
tar cvzf output_files.tgz $OUTPUT_PATH --remove-files
echo "--------------------------------------------------"

echo "Health Check Report Created Successfully"
echo "Path to Report: $REPORT"
echo "--------------------------------------------------"

