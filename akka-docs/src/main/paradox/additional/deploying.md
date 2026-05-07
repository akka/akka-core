---
project.description: How to deploy Akka Cluster to Kubernetes and Docker.
---
# Deploying

## Deploying to Kubernetes

You can deploy to Kubernetes according to the guide and example project for @extref:[Deploying Akka Cluster to Kubernetes](akka-management:kubernetes-deployment/index.html).

### Cluster bootstrap

To take advantage of running inside Kubernetes while forming a cluster,
@extref:[Akka Cluster Bootstrap](akka-management:bootstrap/) helps forming or joining a cluster using Akka Discovery to discover peer nodes. 
with the Kubernetes API or Kubernetes via DNS.  

### Rolling updates

Enable the @extref:[Kubernetes Rolling Updates](akka-management:rolling-updates.html#kubernetes-rolling-updates)
and @extref:[app-version from Deployment](akka-management:rolling-updates.html#app-version-from-deployment)
features from Akka Management for smooth rolling updates.
 
### Resource management

#### CPU requests and limits

To avoid CFS scheduler limits, it is best not to use `resources.limits.cpu` limits, but use `resources.requests.cpu`
configuration instead.

@@@ note

Some resource sizing for Akka and other library dependencies, such as for thread pools or direct memory pools, is based
on the detected number of available processors. This will be the CPU limit, if configured, or otherwise all available
CPU on the underlying Kubernetes node. While it's recommended to not set a CPU limit, this can lead to over-sized
resource allocation. The available processors detected by the JVM can be configured directly using the
`-XX:ActiveProcessorCount` option.

---

**Example**: Akka applications are being deployed to Kubernetes on 16 CPU nodes. Workloads are variable, so to schedule
several pods on each node, a CPU request of 2 is being used. No CPU limit is set, so that pods can burst to more CPU
usage as needed and when available. `-XX:ActiveProcessorCount=4` is added to the JVM options so that thread pools are
sized appropriately for 4 CPU --- rather than the full 16 CPU as detected automatically, and more than the 2 CPU
request, for when the application is active and able to use more resources.

@@@

#### Memory requests and limits

For memory, it's recommended to set both `resources.requests.memory` and `resources.limits.memory` to the same value.
The `-XX:InitialRAMPercentage` and `-XX:MaxRAMPercentage` JVM options can be used to set the heap size relative to the
memory limit.

@@@ note

To account for non-heap memory areas (such as class metadata, threads, code cache, symbols, garbage collection, and
direct memory), it's recommended to set the heap percentage to **70%** of the available memory. This may need to be a
smaller percentage for lower memory limits, or can be a higher percentage for higher memory limits.

@@@

## Deploying to Docker containers

You can use both Akka remoting and Akka Cluster inside Docker containers. Note
that you will need to take special care with the network configuration when using Docker,
described here: @ref:[Akka behind NAT or in a Docker container](../remoting-artery.md#remote-configuration-nat-artery)

For the JVM to run well in a Docker container, there are some general (not Akka specific) parameters that might need tuning:

### Resource constraints

Docker allows [constraining each containers' resource usage](https://docs.docker.com/config/containers/resource_constraints/).

#### Memory

Any memory limits for the Docker container are detected automatically by current JVMs by default. The
`-XX:InitialRAMPercentage` and `-XX:MaxRAMPercentage` JVM options can be used to set the heap size relative to the
memory limit.

@@@ note

To account for non-heap memory areas (such as class metadata, threads, code cache, symbols, garbage collection, and
direct memory), it's recommended to set the heap percentage to **70%** of the available memory. This may need to be a
smaller percentage for lower memory limits, or can be a higher percentage for higher memory limits.

@@@

#### CPU

For multithreaded applications such as the JVM, the CFS scheduler limits are an ill fit, because they will restrict
the allowed CPU usage even when more CPU cycles are available from the host system. This means your application may be
starved of CPU time, but your system appears idle.

For this reason, it is best to avoid `--cpus` and `--cpu-quota` entirely, and instead specify relative container weights using `--cpu-shares` instead.

@@@ note

Some resource sizing for Akka and other library dependencies, such as for thread pools or direct memory pools, is based
on the detected number of available processors. This will be the CPU quota, if configured, or otherwise all CPU
available to Docker. While it's recommended to not set a CPU quota, this can lead to over-sized resource allocation.
The available processors detected by the JVM can be configured directly using the `-XX:ActiveProcessorCount` option.

@@@
