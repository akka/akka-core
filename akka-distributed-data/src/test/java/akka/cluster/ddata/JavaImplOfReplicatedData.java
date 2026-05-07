/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata;

import akka.cluster.UniqueAddress;

public class JavaImplOfReplicatedData extends AbstractReplicatedData<JavaImplOfReplicatedData>
    implements RemovedNodePruning {

  @Override
  public JavaImplOfReplicatedData mergeData(JavaImplOfReplicatedData other) {
    return this;
  }

  @Override
  public scala.collection.immutable.Set<UniqueAddress> modifiedByNodes() {
    return akka.japi.Util.immutableSeq(new java.util.ArrayList<UniqueAddress>()).toSet();
  }

  @Override
  public boolean needPruningFrom(UniqueAddress removedNode) {
    return false;
  }

  @Override
  public JavaImplOfReplicatedData prune(UniqueAddress removedNode, UniqueAddress collapseInto) {
    return this;
  }

  @Override
  public JavaImplOfReplicatedData pruningCleanup(UniqueAddress removedNode) {
    return this;
  }
}
