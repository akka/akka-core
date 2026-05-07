# Java Serialization, Fixed in Akka 2.4.17

### Date

10 February 2017

### Description of Vulnerability

An attacker that can connect to an `ActorSystem` exposed via Akka Remote over TCP can gain remote code execution 
capabilities in the context of the JVM process that runs the ActorSystem if:

 * `JavaSerializer` is enabled (default in Akka 2.4.x)
 * and TLS is disabled *or* TLS is enabled with `akka.remote.netty.ssl.security.require-mutual-authentication = false`
(which is still the default in Akka 2.4.x)
 * or if TLS is enabled with mutual authentication and the authentication keys of a host that is allowed to connect have been compromised, an attacker gained access to a valid certificate (e.g. by compromising a node with certificates issued by the same internal PKI tree to get access of the certificate)
 * regardless of whether `untrusted` mode is enabled or not

Java deserialization is [known to be vulnerable](https://community.microfocus.com/cyberres/fortify/f/fortify-discussions/317555/the-perils-of-java-deserialization) to attacks when attacker can provide arbitrary types.

Akka Remoting uses Java serializer as default configuration which makes it vulnerable in its default form. The documentation of how to disable Java serializer was not complete. The documentation of how to enable mutual authentication was missing (only described in reference.conf).

To protect against such attacks the system should be updated to Akka *2.4.17* or later and be configured with 
[disabled Java serializer](https://doc.akka.io/libraries/akka-core/2.5/remoting.html#disable-java-serializer). Additional protection can be achieved when running in an
untrusted network by enabling @ref:[TLS with mutual authentication](../remote-security.md#remote-tls).

Please subscribe to the [akka-security](https://groups.google.com/forum/#!forum/akka-security) mailing list to be notified promptly about future security issues.

### Severity

The [CVSS](https://en.wikipedia.org/wiki/CVSS) score of this vulnerability is 6.8 (Medium), based on vector [AV:A/AC:M/Au:N/C:C/I:C/A:C/E:F/RL:TF/RC:C](https://nvd.nist.gov/vuln-metrics/cvss/v2-calculator?calculator&amp;version=2&amp;vector=%5C(AV:A/AC:M/Au:N/C:C/I:C/A:C/E:F/RL:TF/RC:C%5C)).

Rationale for the score:

 * AV:A - Best practice is that Akka remoting nodes should only be accessible from the adjacent network, so in good setups, this will be adjacent.
 * AC:M - Any one in the adjacent network can launch the attack with non-special access privileges.
 * C:C, I:C, A:C - Remote Code Execution vulnerabilities are by definition CIA:C.

### Affected Versions

 * Akka *2.4.16* and prior
 * Akka *2.5-M1* (milestone not intended for production)

### Fixed Versions

We have prepared patches for the affected versions, and have released the following versions which resolve the issue: 

 * Akka *2.4.17* (Scala 2.11, 2.12)

Binary and source compatibility has been maintained for the patched releases so the upgrade procedure is as simple as changing the library dependency.

It will also be fixed in 2.5-M2 or 2.5.0-RC1.

### Acknowledgements

We would like to thank Alvaro Munoz at Hewlett Packard Enterprise Security & Adrian Bravo at Workday for their thorough investigation and bringing this issue to our attention.
