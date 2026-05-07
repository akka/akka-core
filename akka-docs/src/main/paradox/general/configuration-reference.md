# Default configuration

Each Akka module has a `reference.conf` file with the default values.

Make your edits/overrides in your `application.conf`. Don't override default values if
you are not sure of the implications. [Akka Config Checker](https://doc.akka.io/libraries/akka-diagnostics/current/config-checker.html)
is a useful tool for finding potential configuration issues.

The purpose of `reference.conf` files is for libraries, like Akka, to define default values that are used if
an application doesn't define a more specific value. It's also a good place to document the existence and
meaning of the configuration properties. One library must not try to override properties in its own `reference.conf`
for properties originally defined by another library's `reference.conf`, because the effective value would be
nondeterministic when loading the configuration.`

<a id="config-akka-actor"></a>
### akka-actor

@@snip [reference.conf](/akka-actor/src/main/resources/reference.conf)

<a id="config-akka-actor-typed"></a>
### akka-actor-typed

@@snip [reference.conf](/akka-actor-typed/src/main/resources/reference.conf)

<a id="config-akka-cluster-typed"></a>
### akka-cluster-typed

@@snip [reference.conf](/akka-cluster-typed/src/main/resources/reference.conf)

<a id="config-akka-cluster"></a>
### akka-cluster

@@snip [reference.conf](/akka-cluster/src/main/resources/reference.conf)

<a id="config-akka-discovery"></a>
### akka-discovery

@@snip [reference.conf](/akka-discovery/src/main/resources/reference.conf)

<a id="config-akka-coordination"></a>
### akka-coordination

@@snip [reference.conf](/akka-coordination/src/main/resources/reference.conf)

<a id="config-akka-multi-node-testkit"></a>
### akka-multi-node-testkit

@@snip [reference.conf](/akka-multi-node-testkit/src/main/resources/reference.conf)

<a id="config-akka-persistence-typed"></a>
### akka-persistence-typed

@@snip [reference.conf](/akka-persistence-typed/src/main/resources/reference.conf)

<a id="config-akka-persistence"></a>
### akka-persistence

@@snip [reference.conf](/akka-persistence/src/main/resources/reference.conf)

<a id="config-akka-persistence-query"></a>
### akka-persistence-query

@@snip [reference.conf](/akka-persistence-query/src/main/resources/reference.conf)

<a id="config-akka-persistence-testkit"></a>
### akka-persistence-testkit

@@snip [reference.conf](/akka-persistence-testkit/src/main/resources/reference.conf)

<a id="config-akka-remote-artery"></a>
### akka-remote artery

@@snip [reference.conf](/akka-remote/src/main/resources/reference.conf) { #artery type=none }

<a id="config-akka-testkit"></a>
### akka-testkit

@@snip [reference.conf](/akka-testkit/src/main/resources/reference.conf)

<a id="config-cluster-metrics"></a>
### akka-cluster-metrics

@@snip [reference.conf](/akka-cluster-metrics/src/main/resources/reference.conf)

<a id="config-cluster-tools"></a>
### akka-cluster-tools

@@snip [reference.conf](/akka-cluster-tools/src/main/resources/reference.conf)

<a id="config-cluster-sharding-typed"></a>
### akka-cluster-sharding-typed

@@snip [reference.conf](/akka-cluster-sharding-typed/src/main/resources/reference.conf)

<a id="config-cluster-sharding"></a>
### akka-cluster-sharding

@@snip [reference.conf](/akka-cluster-sharding/src/main/resources/reference.conf)

<a id="config-distributed-data"></a>
### akka-distributed-data

@@snip [reference.conf](/akka-distributed-data/src/main/resources/reference.conf)

<a id="config-akka-stream"></a>
### akka-stream

@@snip [reference.conf](/akka-stream/src/main/resources/reference.conf)

<a id="config-akka-stream-testkit"></a>
### akka-stream-testkit

@@snip [reference.conf](/akka-stream-testkit/src/main/resources/reference.conf)

