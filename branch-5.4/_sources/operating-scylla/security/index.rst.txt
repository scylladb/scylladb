Security
========

.. toctree::
   :hidden:
 
   security-checklist
   authentication
   runtime-authentication
   create-superuser
   gen-cqlsh-file
   Reset Authenticator Password </troubleshooting/password-reset>
   enable-authorization
   authorization
   certificate-authentication
   rbac-usecase
   client-node-encryption
   node-node-encryption
   generate-certificate
   saslauthd
      

.. panel-box::
  :title: Security
  :id: "getting-started"
  :class: my-panel
   
  * :doc:`ScyllaDB Security Checklist </operating-scylla/security/security-checklist/>`

.. panel-box::
  :title: Authentication and Authorization
  :id: "getting-started"
  :class: my-panel

  * :doc:`Enable Authentication </operating-scylla/security/authentication/>`
  * :doc:`Enable and Disable Authentication Without Downtime </operating-scylla/security/runtime-authentication/>`
  * :doc:`Creating a Custom Superuser </operating-scylla/security/create-superuser/>`
  * :doc:`Generate a cqlshrc File <gen-cqlsh-file>`
  * :doc:`Enable Authorization</operating-scylla/security/enable-authorization/>`
  * :doc:`Role Based Access Control (RBAC) </operating-scylla/security/rbac-usecase/>`
  * :doc:`Grant Authorization CQL Reference </operating-scylla/security/authorization/>`
  * :doc:`Reset Authenticator Password </troubleshooting/password-reset/>`
  * :doc:`Certificate Based Authentication </operating-scylla/security/certificate-authentication/>`

.. panel-box::
  :title: Encryption
  :id: "getting-started"
  :class: my-panel

  * :doc:`Encryption: Data in Transit Client to Node </operating-scylla/security/client-node-encryption/>`
  * :doc:`Encryption: Data in Transit Node to Node </operating-scylla/security/node-node-encryption/>`
  * :doc:`Generating a self-signed Certificate Chain Using openssl </operating-scylla/security/generate-certificate/>`
  * `Encryption at Rest <https://enterprise.docs.scylladb.com/stable/operating-scylla/security/encryption-at-rest.html>`_ available in `ScyllaDB Enterprise <https://enterprise.docs.scylladb.com/>`_

Also check out the `Security Features lesson <https://university.scylladb.com/courses/scylla-operations/lessons/security-features/topic/security-features/>`_ on Scylla University.
