# DNS Extension

@@@ warning

`async-dns` does not support:

* [Local hosts file](https://github.com/akka/akka-core/issues/25846) e.g. `/etc/hosts` on Unix systems
* The [nsswitch.conf](https://linux.die.net/man/5/nsswitch.conf) file (no plan to support)

Additionally, while search domains are supported through configuration, detection of the system configured
[Search domains](https://github.com/akka/akka-core/issues/25825) is only supported on systems that provide this 
configuration through a `/etc/resolv.conf` file, i.e. it isn't supported on Windows or OSX, and none of the 
environment variables that are usually supported on most \*nix OSes are supported.

@@@

@@@ note

The `async-dns` API is marked as `ApiMayChange` as more information is expected to be added to the protocol.

@@@

@@@ warning

The ability to plugin in a custom DNS implementation is expected to be removed in future versions of Akka.
Users should pick one of the built in extensions.

@@@

Akka DNS is a pluggable way to interact with DNS. Implementations must implement `akka.io.DnsProvider` and provide a configuration
block that specifies the implementation via `provider-object`.

@@@ note { title="DNS via Akka Discovery" }

@ref[Akka Discovery](discovery/index.md) can be backed by the Akka DNS implementation and provides a more general API for service lookups which is not limited to domain name lookup.

@@@

To select which `DnsProvider` to use set `akka.io.dns.resolver ` to the location of the configuration.

There are currently two implementations:

* `inet-address` - Based on the JDK's `InetAddress`. Using this will be subject to both the JVM's DNS cache and its built in one.
* `async-dns` - A native implemention of the DNS protocol that does not use any JDK classes or caches.

`inet-address` is the default implementation as it pre-dates `async-dns`, `async-dns` will likely become the default in the next major release.

DNS lookups can be done via the `DNS` extension:

Scala
:  @@snip [DnsCompileOnlyDocSpec.scala](/akka-docs/src/test/scala/docs/actor/io/dns/DnsCompileOnlyDocSpec.scala) { #resolve }

Java
:  @@snip [DnsCompileOnlyDocTest.java](/akka-docs/src/test/java/jdocs/actor/io/dns/DnsCompileOnlyDocTest.java) { #resolve }

Alternatively the `IO(Dns)` actor can be interacted with directly. However this exposes the different protocols of the DNS provider.
`inet-adddress` uses `Dns.Resolve` and `Dns.Resolved` where as the `async-dns` uses `DnsProtocol.Resolve` and `DnsProtocol.Resolved`. 
The reason for the difference is `inet-address` predates `async-dns` and `async-dns` exposes additional information such as SRV records 
and it wasn't possible to evolve the original API in a backward compatible way.

Inet-Address API:

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/actor/io/dns/DnsCompileOnlyDocSpec.scala) { #actor-api-inet-address }

Java
:  @@snip [DnsCompileOnlyDocTest.java](/akka-docs/src/test/java/jdocs/actor/io/dns/DnsCompileOnlyDocTest.java) { #actor-api-inet-address }

Async-DNS API:

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/actor/io/dns/DnsCompileOnlyDocSpec.scala) { #actor-api-async }

Java
:  @@snip [DnsCompileOnlyDocTest.java](/akka-docs/src/test/java/jdocs/actor/io/dns/DnsCompileOnlyDocTest.java) { #actor-api-async }

The Async DNS provider has the following advantages:

* No JVM DNS caching. It is expected that future versions will expose more caching related information.
* No blocking. `InetAddress` resolving is a blocking operation.
* Exposes `SRV`, `A` and `AAAA` records.


## SRV Records

To get DNS SRV records `akka.io.dns.resolver` must be set to `async-dns` and `DnsProtocol.Resolve`'s requestType
must be set to `DnsProtocol.Srv` 

Scala
:  @@snip [IODocSpec.scala](/akka-docs/src/test/scala/docs/actor/io/dns/DnsCompileOnlyDocSpec.scala) { #srv }

Java
:  @@snip [DnsCompileOnlyDocTest.java](/akka-docs/src/test/java/jdocs/actor/io/dns/DnsCompileOnlyDocTest.java) { #srv }

The `DnsProtocol.Resolved` will contain `akka.io.dns.SRVRecord`s.






