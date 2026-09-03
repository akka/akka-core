#!/bin/bash
set -e

# Generates a second, independent root CA plus one node keyset issued by it, used to test
# CA rotation with RotatingKeysSSLEngineProvider: a real scenario needs two nodes issued by
# two different CAs to both trust each other via a two-CA bundle. This is kept separate
# from genca.sh/gencerts.sh (which regenerate the whole `exampleca` fixture set) so that
# adding this second CA does not touch any existing fixture.

mkdir -p rotation-ca2
cd rotation-ca2

rm -f exampleca2.crt artery-node004.example.com.crt artery-node004.example.com.pem ca-bundle.crt

openssl ecparam -genkey -name prime256v1 -out ca2.key

openssl req -x509 -new -key ca2.key -sha256 -days 9999 \
  -out exampleca2.crt \
  -subj "/C=US/ST=California/L=San Francisco/O=Example Company/OU=Example Org/CN=exampleCA2" \
  -addext "keyUsage=critical,keyCertSign" \
  -addext "basicConstraints=critical,CA:true"

openssl genrsa -out node2.key 2048

openssl req -new -key node2.key -out node2.csr \
  -subj "/C=US/ST=California/L=San Francisco/O=Example Company/OU=Example Org/CN=artery-node004.example.com"

cat > node2.ext <<EOF
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:artery-node004.example.com,DNS:artery-node.example.com
EOF

openssl x509 -req -in node2.csr -CA exampleca2.crt -CAkey ca2.key -CAcreateserial \
  -out artery-node004.example.com.crt -days 3650 -sha256 -extfile node2.ext

# RotatingKeysSSLEngineProvider requires the node private key as PKCS#1 or non-encrypted
# PKCS#8 PEM -- not the CA's key algorithm, which can be anything (EC here, as for `exampleca`).
openssl rsa -in node2.key -out artery-node004.example.com.pem

# Bundle both CAs together so a node issued by either one is trusted -- this is the
# file RotatingKeysSSLEngineProvider's ca-cert-file points at during a rotation overlap.
cat ../exampleca.crt exampleca2.crt > ca-bundle.crt

rm -f ca2.key node2.key node2.csr node2.ext exampleca2.srl
