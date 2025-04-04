# How to enable SASL/SHA-256 or SASL/SHA-512

Create a JAAS file, like `data/jaas/sasl.conf` with the following contents:

```
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin" user_admin="admin";
  org.apache.kafka.common.security.scram.Plain required username="admin" password="admin";
  org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin";
};

KafkaClient {
  org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";
};
```

(`username/password` are used by the broker to connect to other brokers, while `user_*` define valid users).

Ensure the following mapping is enabled in the docker-compose volumes:

```
- ./data/jaas:/var/jaas
```

If you need to run the ACL tools within the docker container, you will need a `admin.conf` structured like this:

```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
```

Add the user by opening the shell in the Docker container and by doing:

```
/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=client]' --entity-type users --entity-name client --command-config admin.config
```

Permission can be added with a command similar to the following one:

```
/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:client --topic temp --operation all --command-config admin.config
```
