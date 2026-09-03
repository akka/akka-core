#!/bin/bash
set -e

# Generates a "same subject DN" CA rotation fixture: two CA certificates that share the
# exact same subject DN but have different keys (as cert-manager produces when it renews
# a CA Certificate in place), plus one node keyset issued by the *new* CA. Used to test
# that PemManagersProvider.findIssuer disambiguates by signature, not by DN alone.
#
# Kept separate from genca.sh/gencerts.sh so that adding this fixture does not touch any
# existing certificate.

mkdir -p rotation-same-dn
cd rotation-same-dn

rm -f ca-old.crt ca-new.crt node.crt node.pem

CA_SUBJ="/C=US/ST=California/L=San Francisco/O=Example Company/OU=Example Org/CN=rotatedCA"

# Old CA
openssl ecparam -genkey -name prime256v1 -out ca-old.key
openssl req -x509 -new -key ca-old.key -sha256 -days 9999 \
  -out ca-old.crt \
  -subj "$CA_SUBJ" \
  -addext "keyUsage=critical,keyCertSign" \
  -addext "basicConstraints=critical,CA:true"

# New CA: same subject DN, fresh key (this is the in-place renewal)
openssl ecparam -genkey -name prime256v1 -out ca-new.key
openssl req -x509 -new -key ca-new.key -sha256 -days 9999 \
  -out ca-new.crt \
  -subj "$CA_SUBJ" \
  -addext "keyUsage=critical,keyCertSign" \
  -addext "basicConstraints=critical,CA:true"

# Node keyset, issued by the NEW CA
openssl genrsa -out node.key 2048
openssl req -new -key node.key -out node.csr \
  -subj "/C=US/ST=California/L=San Francisco/O=Example Company/OU=Example Org/CN=rotated-node.example.com"

cat > node.ext <<EOF
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:rotated-node.example.com
EOF

openssl x509 -req -in node.csr -CA ca-new.crt -CAkey ca-new.key -CAcreateserial \
  -out node.crt -days 3650 -sha256 -extfile node.ext

# RotatingKeysSSLEngineProvider requires the node private key as PKCS#1 or non-encrypted
# PKCS#8 PEM.
openssl rsa -in node.key -out node.pem

rm -f ca-old.key ca-new.key node.key node.csr node.ext ca-new.srl
