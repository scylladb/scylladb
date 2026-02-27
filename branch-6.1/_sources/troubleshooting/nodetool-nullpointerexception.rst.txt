Nodetool Throws NullPointerException
=====================================================

Problem
^^^^^^^
A NullPointerException is thrown when you run ``nodetool``. For example:

.. code-block:: console

   java.lang.NullPointerException
       at org.apache.cassandra.config.DatabaseDescriptor.getDiskFailurePolicy(DatabaseDescriptor.java:1881)
       at org.apache.cassandra.utils.JVMStabilityInspector.inspectThrowable(JVMStabilityInspector.java:82)
       at org.apache.cassandra.io.util.FileUtils.<clinit>(FileUtils.java:79)
       at org.apache.cassandra.utils.FBUtilities.getToolsOutputDirectory(FBUtilities.java:824)
       at org.apache.cassandra.tools.NodeTool.printHistory(NodeTool.java:200)
       at org.apache.cassandra.tools.NodeTool.main(NodeTool.java:168)


Solution
^^^^^^^^
NullPointerException may have more than one cause. When you run the nodetool utility, the exception may indicate
an unsupported Java version installed on your machine. Nodetool supports Java 7 and 8.

#. Check your Java version with the ``java --version`` command.
#. If nodetool does not support your version, replace it with Java 7 or 8.

