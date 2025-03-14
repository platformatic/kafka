# Setup SSL

Prepare the folder:

`mkdir -p ssl`

Generate the CA:

```
openssl req -new -newkey rsa:4096 -days 365 -x509 -subj "/CN=docker" -keyout ssl/ca.key -out ssl/ca.cert -nodes
```

Generate the server keystore:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/server.keystore.jks -alias server -genkey -keyalg RSA -validity 365 -dname CN=server -ext SAN=DNS:server
```

Create the trust store:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/server.truststore.jks -alias ca -importcert -file ssl/ca.cert
```

Create a sign request:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/server.keystore.jks -alias server -certreq -file ssl/server.csr
```

Sign the certificate with the CA:

```
openssl x509 -req -CA ssl/ca.cert -CAkey ssl/ca.key -in ssl/server.csr -out ssl/server.pem -days 365
```

Import the CA and the signed certificate into the keystore:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/server.keystore.jks -alias ca -importcert -file ssl/ca.cert
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/server.keystore.jks -alias server -importcert -file ssl/server.pem
```

## Setup mTLS

Create the client certificate:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/client.keystore.jks -alias client -genkey -keyalg RSA -validity 365 -dname CN=client -ext SAN=DNS:client
```

Create a sign request:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/client.keystore.jks -alias client -certreq -file ssl/client.csr
```

Sign the certificate with the CA:

```
openssl x509 -req -CA ssl/ca.cert -CAkey ssl/ca.key -in ssl/client.csr -out ssl/client.pem -days 365
```

Export the certificate private key:

```
keytool -noprompt -importkeystore -srckeystore ssl/client.keystore.jks -srcalias client -srcstorepass 12345678 -deststorepass 12345678 -destkeypass 12345678 -destkeystore ssl/client.p12 -deststoretype PKCS12
openssl pkcs12 -nocerts -legacy -in ssl/client.p12 -out ssl/client.key -passin pass:12345678 -nodes
```

Import the CA and the signed certificate into the keystore:

```
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/client.keystore.jks -alias ca -importcert -file ssl/ca.cert
keytool -storepass 12345678 -keypass 12345678 -noprompt -keystore ssl/client.keystore.jks -alias server -importcert -file ssl/client.pem
```
