# Setup SSL

Prepare the folder:

`mkdir -p ssl`

Generate the CA:

```
openssl req -new -newkey rsa:4096 -days 365 -x509 -subj "/CN=docker" -keyout data/ssl/ca.key -out data/ssl/ca.cert -nodes
```

Generate the server keystore:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/server.keystore.jks -alias server -genkey -keyalg RSA -validity 365 -dname CN=server -ext SAN=DNS:server
```

Create the trust store:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/server.truststore.jks -alias ca -importcert -file data/ssl/ca.cert
```

Create a sign request:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/server.keystore.jks -alias server -certreq -file data/ssl/server.csr
```

Sign the certificate with the CA:

```
openssl x509 -req -CA data/ssl/ca.cert -CAkey data/ssl/ca.key -in data/ssl/server.csr -out data/ssl/server.pem -days 365
```

Import the CA and the signed certificate into the keystore:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/server.keystore.jks -alias ca -importcert -file data/ssl/ca.cert
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/server.keystore.jks -alias server -importcert -file data/ssl/server.pem
```

## Setup mTLS

Create the client certificate:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/client.keystore.jks -alias client -genkey -keyalg RSA -validity 365 -dname CN=client -ext SAN=DNS:client
```

Create a sign request:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/client.keystore.jks -alias client -certreq -file data/ssl/client.csr
```

Sign the certificate with the CA:

```
openssl x509 -req -CA data/ssl/ca.cert -CAkey data/ssl/ca.key -in data/ssl/client.csr -out data/ssl/client.pem -days 365
```

Export the certificate private key:

```
keytool -noprompt -importkeystore -srckeystore data/ssl/client.keystore.jks -srcalias client -srcstorepass 12345678 -deststorepass 12345678 -destkeypass 12345678 -destkeystore data/ssl/client.p12 -deststoretype PKCS12
openssl pkcs12 -nocerts -legacy -in data/ssl/client.p12 -out data/ssl/client.key -passin pass:12345678 -nodes
```

Import the CA and the signed certificate into the keystore:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/client.keystore.jks -alias ca -importcert -file data/ssl/ca.cert
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore data/ssl/client.keystore.jks -alias server -importcert -file data/ssl/client.pem
```
