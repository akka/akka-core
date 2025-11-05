# Private keys for tests

Created using:
```shell
# Eliptic curve key
$ openssl ecparam -out ecdsa.pem -name secp256r1 -genkey
$ openssl pkcs8 -topk8 -nocrypt -in ecdsa.pem -out pkcs8-ecdsa.pem 
```
