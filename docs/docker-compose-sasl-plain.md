# How to enable SASL/PLAIN

Create a JAAS file, like `playground/sasl.conf` with the following contents:

```
KafkaServer {
 org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin" password="admin"
  user_admin="admin"
  user_client="client";
};

KafkaClient {
  org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";
};
```

(`username/password` are used by the broker to connect to other brokers, while `user_*` define valid users).

Ensure the following mapping is enabled in the docker-compose volumes:

```
- ./playground/jaas.conf:/var/jaas/jaas.conf
```

If you need to run the ACL tools within the docker container, you will need a `admin.conf` structured like this:

```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
```

Permission can be added with a command similar to the following one:

```
/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9092 --add --allow-principal User:client --topic temp --operation all --command-config admin.config
```
