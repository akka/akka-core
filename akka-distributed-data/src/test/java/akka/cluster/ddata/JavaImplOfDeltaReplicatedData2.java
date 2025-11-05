/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata;

import java.util.Optional;

// different delta type
public class JavaImplOfDeltaReplicatedData2
    extends AbstractDeltaReplicatedData<
        JavaImplOfDeltaReplicatedData2, JavaImplOfDeltaReplicatedData2.Delta> {

  public static class Delta extends AbstractReplicatedData<Delta>
      implements ReplicatedDelta, RequiresCausalDeliveryOfDeltas {
    @Override
    public Delta mergeData(Delta other) {
      return this;
    }

    @Override
    public JavaImplOfDeltaReplicatedData2 zero() {
      return new JavaImplOfDeltaReplicatedData2();
    }
  }

  @Override
  public JavaImplOfDeltaReplicatedData2 mergeData(JavaImplOfDeltaReplicatedData2 other) {
    return this;
  }

  @Override
  public JavaImplOfDeltaReplicatedData2 mergeDeltaData(Delta other) {
    return this;
  }

  @Override
  public Optional<Delta> deltaData() {
    return Optional.empty();
  }

  @Override
  public JavaImplOfDeltaReplicatedData2 resetDelta() {
    return this;
  }
}
