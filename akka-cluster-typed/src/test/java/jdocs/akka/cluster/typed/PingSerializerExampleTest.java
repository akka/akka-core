/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.typed;

import akka.actor.ExtendedActorSystem;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorRefResolver;
import akka.actor.typed.javadsl.Adapter;
import akka.serialization.SerializerWithStringManifest;
import java.nio.charset.StandardCharsets;

public class PingSerializerExampleTest {

  public class Pong {}

  public class Ping {
    public final akka.actor.typed.ActorRef<Pong> replyTo;

    public Ping(ActorRef<Pong> replyTo) {
      this.replyTo = replyTo;
    }
  }

  // #serializer
  public class PingSerializer extends SerializerWithStringManifest {

    final ExtendedActorSystem system;
    final ActorRefResolver actorRefResolver;

    static final String PING_MANIFEST = "a";
    static final String PONG_MANIFEST = "b";

    PingSerializer(ExtendedActorSystem system) {
      this.system = system;
      actorRefResolver = ActorRefResolver.get(Adapter.toTyped(system));
    }

    @Override
    public int identifier() {
      return 97876;
    }

    @Override
    public String manifest(Object obj) {
      if (obj instanceof Ping) return PING_MANIFEST;
      else if (obj instanceof Pong) return PONG_MANIFEST;
      else
        throw new IllegalArgumentException(
            "Can't serialize object of type "
                + obj.getClass()
                + " in ["
                + getClass().getName()
                + "]");
    }

    @Override
    public byte[] toBinary(Object obj) {
      if (obj instanceof Ping)
        return actorRefResolver
            .toSerializationFormat(((Ping) obj).replyTo)
            .getBytes(StandardCharsets.UTF_8);
      else if (obj instanceof Pong) return new byte[0];
      else
        throw new IllegalArgumentException(
            "Can't serialize object of type "
                + obj.getClass()
                + " in ["
                + getClass().getName()
                + "]");
    }

    @Override
    public Object fromBinary(byte[] bytes, String manifest) {
      if (PING_MANIFEST.equals(manifest)) {
        String str = new String(bytes, StandardCharsets.UTF_8);
        ActorRef<Pong> ref = actorRefResolver.resolveActorRef(str);
        return new Ping(ref);
      } else if (PONG_MANIFEST.equals(manifest)) {
        return new Pong();
      } else {
        throw new IllegalArgumentException("Unable to handle manifest: " + manifest);
      }
    }
  }
  // #serializer
}
