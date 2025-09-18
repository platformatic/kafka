#!/bin/sh

# Remove existing configuration
sed -i '/listener\.name\.sasl_plaintext\.[^.]*\.sasl\.jaas\.config/d' /opt/bitnami/kafka/config/server.properties

# Add new configuration
cat >> /opt/bitnami/kafka/config/server.properties << 'EOF'

# SASL JAAS configuration
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin" user_admin="admin";
listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin";
listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin";
listener.name.sasl_plaintext.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
EOF