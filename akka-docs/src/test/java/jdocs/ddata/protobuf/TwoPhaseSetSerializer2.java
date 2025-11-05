/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.ddata.protobuf;

// #serializer
import akka.actor.ExtendedActorSystem;
import akka.cluster.ddata.GSet;
import akka.cluster.ddata.protobuf.AbstractSerializationSupport;
import akka.cluster.ddata.protobuf.ReplicatedDataSerializer;
import docs.ddata.protobuf.msg.TwoPhaseSetMessages;
import docs.ddata.protobuf.msg.TwoPhaseSetMessages.TwoPhaseSet2.Builder;
import jdocs.ddata.TwoPhaseSet;

public class TwoPhaseSetSerializer2 extends AbstractSerializationSupport {

  private final ExtendedActorSystem system;
  private final ReplicatedDataSerializer replicatedDataSerializer;

  public TwoPhaseSetSerializer2(ExtendedActorSystem system) {
    this.system = system;
    this.replicatedDataSerializer = new ReplicatedDataSerializer(system);
  }

  @Override
  public ExtendedActorSystem system() {
    return this.system;
  }

  @Override
  public boolean includeManifest() {
    return false;
  }

  @Override
  public int identifier() {
    return 99998;
  }

  @Override
  public byte[] toBinary(Object obj) {
    if (obj instanceof TwoPhaseSet) {
      return twoPhaseSetToProto((TwoPhaseSet) obj).toByteArray();
    } else {
      throw new IllegalArgumentException("Can't serialize object of type " + obj.getClass());
    }
  }

  @Override
  public Object fromBinaryJava(byte[] bytes, Class<?> manifest) {
    return twoPhaseSetFromBinary(bytes);
  }

  protected TwoPhaseSetMessages.TwoPhaseSet2 twoPhaseSetToProto(TwoPhaseSet twoPhaseSet) {
    Builder b = TwoPhaseSetMessages.TwoPhaseSet2.newBuilder();
    if (!twoPhaseSet.adds.isEmpty())
      b.setAdds(otherMessageToProto(twoPhaseSet.adds).toByteString());
    if (!twoPhaseSet.removals.isEmpty())
      b.setRemovals(otherMessageToProto(twoPhaseSet.removals).toByteString());
    return b.build();
  }

  @SuppressWarnings("unchecked")
  protected TwoPhaseSet twoPhaseSetFromBinary(byte[] bytes) {
    try {
      TwoPhaseSetMessages.TwoPhaseSet2 msg = TwoPhaseSetMessages.TwoPhaseSet2.parseFrom(bytes);

      GSet<String> adds = GSet.create();
      if (msg.hasAdds()) adds = (GSet<String>) otherMessageFromBinary(msg.getAdds().toByteArray());

      GSet<String> removals = GSet.create();
      if (msg.hasRemovals())
        adds = (GSet<String>) otherMessageFromBinary(msg.getRemovals().toByteArray());

      return new TwoPhaseSet(adds, removals);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
// #serializer
