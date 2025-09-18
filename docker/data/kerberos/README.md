To create `kafka.keytab`:

```
ktutil
addent -password -p admin/localhost@example.com -k 1 -e aes256-cts-hmac-sha1-96
write_kt kafka.keytab
quit
```

On Mac, use `ktutil` from `krb5`, installed via Homebrew
