# Classic Cluster Metrics Extension

## Module info

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

To use Cluster Metrics Extension, you must add the following dependency in your project:

@@dependency[sbt,Maven,Gradle] {
  bomGroup=com.typesafe.akka bomArtifact=akka-bom_$scala.binary.version$ bomVersionSymbols=AkkaVersion
  symbol1=AkkaVersion
  value1="$akka.version$"
  group=com.typesafe.akka
  artifact=akka-cluster-metrics_$scala.binary.version$
  version=AkkaVersion
}

and add the following configuration stanza to your `application.conf`
:

```
akka.extensions = [ "akka.cluster.metrics.ClusterMetricsExtension" ]
```

@@project-info{ projectId="akka-cluster-metrics" }

## Introduction

The member nodes of the cluster can collect system health metrics and publish that to other cluster nodes
and to the registered subscribers on the system event bus with the help of Cluster Metrics Extension.

Cluster metrics information is primarily used for load-balancing routers,
and can also be used to implement advanced metrics-based node life cycles,
such as "Node Let-it-crash" when CPU steal time becomes excessive.

Cluster members with status @ref:[WeaklyUp](typed/cluster-membership.md#weaklyup-members), if that feature is enabled,
will participate in Cluster Metrics collection and dissemination.

## Metrics Collector

Metrics collection is delegated to an implementation of `akka.cluster.metrics.MetricsCollector`.

Different collector implementations provide different subsets of metrics published to the cluster.
Certain message routing and let-it-crash functions may not work when Sigar is not provisioned.

Cluster metrics extension comes with two built-in collector implementations:

1. `akka.cluster.metrics.SigarMetricsCollector`, which requires Sigar provisioning, and is more rich/precise
2. `akka.cluster.metrics.JmxMetricsCollector`, which is used as fall back, and is less rich/precise

You can also plug-in your own metrics collector implementation.

By default, metrics extension will use collector provider fall back and will try to load them in this order:

1. configured user-provided collector
2. built-in `akka.cluster.metrics.SigarMetricsCollector`
3. and finally `akka.cluster.metrics.JmxMetricsCollector`

## Metrics Events

Metrics extension periodically publishes current snapshot of the cluster metrics to the node system event bus.

The publication interval is controlled by the `akka.cluster.metrics.collector.sample-interval` setting.

The payload of the `akka.cluster.metrics.ClusterMetricsChanged` event will contain
latest metrics of the node as well as other cluster member nodes metrics gossip
which was received during the collector sample interval.

You can subscribe your metrics listener actors to these events in order to implement custom node lifecycle:

Scala
:  @@@ vars
```scala
ClusterMetricsExtension(system).subscribe(metricsListenerActor)
```
@@@

Java
:  @@@ vars
```java
ClusterMetricsExtension.get(system).subscribe(metricsListenerActor);
```
@@@

## Hyperic Sigar Provisioning

Both user-provided and built-in metrics collectors can optionally use [Hyperic Sigar](https://github.com/hyperic/sigar)
for a wider and more accurate range of metrics compared to what can be retrieved from ordinary JMX MBeans.

Sigar is using a native o/s library, and requires library provisioning, i.e.
deployment, extraction and loading of the o/s native library into JVM at runtime.

User can provision Sigar classes and native library in one of the following ways:

 1. Use [Kamon sigar-loader](https://github.com/kamon-io/sigar-loader) as a project dependency for the user project.
Metrics extension will extract and load sigar library on demand with help of Kamon sigar provisioner.
 2. Use [Kamon sigar-loader](https://github.com/kamon-io/sigar-loader) as java agent: `java -javaagent:/path/to/sigar-loader.jar`.
Kamon sigar loader agent will extract and load sigar library during JVM start.
 3. Place `sigar.jar` on the `classpath` and Sigar native library for the o/s on the `java.library.path`.
User is required to manage both project dependency and library deployment manually.

@@@ warning

When using [Kamon sigar-loader](https://github.com/kamon-io/sigar-loader) and running multiple
instances of the same application on the same host, you have to make sure that sigar library is extracted to a
unique per instance directory. You can control the extract directory with the
`akka.cluster.metrics.native-library-extract-folder` configuration setting.

@@@

To enable usage of Sigar you can add the following dependency to the user project:

@@dependency[sbt,Maven,Gradle] {
  group="io.kamon"
  artifact="sigar-loader"
  version="$sigar_loader.version$"
}

You can download Kamon sigar-loader from [Maven Central](https://search.maven.org/search?q=sigar-loader)

## Adaptive Load Balancing

The `AdaptiveLoadBalancingPool` / `AdaptiveLoadBalancingGroup` performs load balancing of messages to cluster nodes based on the cluster metrics data.
It uses random selection of routees with probabilities derived from the remaining capacity of the corresponding node.
It can be configured to use a specific MetricsSelector to produce the probabilities, a.k.a. weights:

* `heap` / `HeapMetricsSelector` - Used and max JVM heap memory. Weights based on remaining heap capacity; (max - used) / max
* `load` / `SystemLoadAverageMetricsSelector` - System load average for the past 1 minute, corresponding value can be found in `top` of Linux systems. The system is possibly nearing a bottleneck if the system load average is nearing number of cpus/cores. Weights based on remaining load capacity; 1 - (load / processors)
* `cpu` / `CpuMetricsSelector` - CPU utilization in percentage, sum of User + Sys + Nice + Wait. Weights based on remaining cpu capacity; 1 - utilization
* `mix` / `MixMetricsSelector` - Combines heap, cpu and load. Weights based on mean of remaining capacity of the combined selectors.
* Any custom implementation of `akka.cluster.metrics.MetricsSelector`

The collected metrics values are smoothed with [exponential weighted moving average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average). In the @ref:[Cluster configuration](cluster-usage.md#cluster-configuration) you can adjust how quickly past data is decayed compared to new data.

Let's take a look at this router in action. What can be more demanding than calculating factorials?

The backend worker that performs the factorial calculation:

Scala
:  @@snip [FactorialBackend.scala](/akka-docs/src/test/scala/docs/cluster/FactorialBackend.scala) { #backend }

Java
:  @@snip [FactorialBackend.java](/akka-docs/src/test/java/jdocs/cluster/FactorialBackend.java) { #backend }

The frontend that receives user jobs and delegates to the backends via the router:

Scala
:  @@snip [FactorialFrontend.scala](/akka-docs/src/test/scala/docs/cluster/FactorialFrontend.scala) { #frontend }

Java
:  @@snip [FactorialFrontend.java](/akka-docs/src/test/java/jdocs/cluster/FactorialFrontend.java) { #frontend }

As you can see, the router is defined in the same way as other routers, and in this case it is configured as follows:

```
akka.actor.deployment {
  /factorialFrontend/factorialBackendRouter = {
    # Router type provided by metrics extension.
    router = cluster-metrics-adaptive-group
    # Router parameter specific for metrics extension.
    # metrics-selector = heap
    # metrics-selector = load
    # metrics-selector = cpu
    metrics-selector = mix
    #
    routees.paths = ["/user/factorialBackend"]
    cluster {
      enabled = on
      use-roles = ["backend"]
      allow-local-routees = off
    }
  }
}
```

It is only `router` type and the `metrics-selector` parameter that is specific to this router,
other things work in the same way as other routers.

The same type of router could also have been defined in code:

Scala
:  @@snip [FactorialFrontend.scala](/akka-docs/src/test/scala/docs/cluster/FactorialFrontend.scala) { #router-lookup-in-code #router-deploy-in-code }

Java
:  @@snip [FactorialFrontend.java](/akka-docs/src/test/java/jdocs/cluster/FactorialFrontend.java) { #router-lookup-in-code #router-deploy-in-code }

## Subscribe to Metrics Events

It is possible to subscribe to the metrics events directly to implement other functionality.

Scala
:  @@snip [MetricsListener.scala](/akka-docs/src/test/scala/docs/cluster/MetricsListener.scala) { #metrics-listener }

Java
:  @@snip [MetricsListener.java](/akka-docs/src/test/java/jdocs/cluster/MetricsListener.java) { #metrics-listener }

## Custom Metrics Collector

Metrics collection is delegated to the implementation of `akka.cluster.metrics.MetricsCollector`

You can plug-in your own metrics collector instead of built-in
`akka.cluster.metrics.SigarMetricsCollector` or `akka.cluster.metrics.JmxMetricsCollector`.

Look at those two implementations for inspiration.

Custom metrics collector implementation class must be specified in the
`akka.cluster.metrics.collector.provider` configuration property.

## Configuration

The Cluster metrics extension can be configured with the following properties:

@@snip [reference.conf](/akka-cluster-metrics/src/main/resources/reference.conf)
