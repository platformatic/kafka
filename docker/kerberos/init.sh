#!/bin/sh
set -e

# Setup KDC if needed
if [ ! -f /var/lib/krb5kdc/principal ]; then

  echo "Setting up KDC ..."
  kdb5_util create -s -P password

  # # ACL file
  echo "*/admin@EXAMPLE.COM *" > /etc/krb5kdc/kadm5.acl

  # Create principals
  kadmin.local -q "addprinc -pw admin admin@EXAMPLE.COM" # Main administrator
  kadmin.local -q "addprinc -randkey broker/broker-sasl-kerberos@EXAMPLE.COM" # Kafka broker
  kadmin.local -q "addprinc -randkey admin-keytab@EXAMPLE.COM" # Client with keytab
  kadmin.local -q "addprinc -pw admin admin-password@EXAMPLE.COM" # Client with password

  # Generate keytabs
  kadmin.local -q "ktadd -k /data/broker.keytab broker/broker-sasl-kerberos@EXAMPLE.COM"
  kadmin.local -q "ktadd -k /data/admin.keytab admin-keytab@EXAMPLE.COM"

  # Allow other containers to read the keytab files
  chown -R ubuntu:ubuntu /data
  chmod -R 755 /data
fi  

krb5kdc
kadmind -nofork