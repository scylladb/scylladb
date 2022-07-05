.. Note:: 

   Make sure to use the same Scylla **patch release** on the new/replaced node, to match the rest of the cluster. It is not recommended to add a new node with a different release to the cluster.
   For example, use the following for installing Scylla patch release (use your deployed version)

   * Scylla Enterprise - ``sudo yum install scylla-enterprise-2018.1.9``
   
   * Scylla open source - ``sudo yum install scylla-3.0.3``


