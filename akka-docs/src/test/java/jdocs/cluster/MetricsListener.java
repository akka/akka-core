/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster;

// #metrics-listener
import akka.actor.AbstractActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.metrics.ClusterMetricsChanged;
import akka.cluster.metrics.ClusterMetricsExtension;
import akka.cluster.metrics.NodeMetrics;
import akka.cluster.metrics.StandardMetrics;
import akka.cluster.metrics.StandardMetrics.Cpu;
import akka.cluster.metrics.StandardMetrics.HeapMemory;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class MetricsListener extends AbstractActor {
  LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  Cluster cluster = Cluster.get(getContext().getSystem());

  ClusterMetricsExtension extension = ClusterMetricsExtension.get(getContext().getSystem());

  // Subscribe unto ClusterMetricsEvent events.
  @Override
  public void preStart() {
    extension.subscribe(getSelf());
  }

  // Unsubscribe from ClusterMetricsEvent events.
  @Override
  public void postStop() {
    extension.unsubscribe(getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            ClusterMetricsChanged.class,
            clusterMetrics -> {
              for (NodeMetrics nodeMetrics : clusterMetrics.getNodeMetrics()) {
                if (nodeMetrics.address().equals(cluster.selfAddress())) {
                  logHeap(nodeMetrics);
                  logCpu(nodeMetrics);
                }
              }
            })
        .match(
            CurrentClusterState.class,
            message -> {
              // Ignore.
            })
        .build();
  }

  void logHeap(NodeMetrics nodeMetrics) {
    HeapMemory heap = StandardMetrics.extractHeapMemory(nodeMetrics);
    if (heap != null) {
      log.info("Used heap: {} MB", ((double) heap.used()) / 1024 / 1024);
    }
  }

  void logCpu(NodeMetrics nodeMetrics) {
    Cpu cpu = StandardMetrics.extractCpu(nodeMetrics);
    if (cpu != null && cpu.systemLoadAverage().isDefined()) {
      log.info("Load: {} ({} processors)", cpu.systemLoadAverage().get(), cpu.processors());
    }
  }
}
// #metrics-listener
