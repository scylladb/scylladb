# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure("2") do |config|
  # CentOS 7 with Virtualbox guest additions
  config.vm.box = "bento/centos-7.3"

  # config.vm.network "forwarded_port", guest: 80, host: 8080

  config.vm.synced_folder ".", "/vagrant", type: "virtualbox"

  config.vm.define "scylla" do |scylla|
    scylla.vm.hostname = "scylla"
  end

  config.vm.provider "virtualbox" do |vb|
    # Display the VirtualBox GUI when booting the machine
    # vb.gui = true

    # Customize the amount of memory and CPUs on the VM:
    vb.memory = "2048"
    vb.cpus = 2
  end

  config.vm.provision "shell", inline: <<-SHELL
    rpm --import http://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-7
    bash /vagrant/scripts/scylla_install_pkg --repo http://downloads.scylladb.com/rpm/centos/scylla-1.7.repo
    scylla_dev_mode_setup --developer-mode 1
    systemctl enable scylla-server
    systemctl enable scylla-jmx
    systemctl start scylla-server
    systemctl start scylla-jmx
  SHELL
end
