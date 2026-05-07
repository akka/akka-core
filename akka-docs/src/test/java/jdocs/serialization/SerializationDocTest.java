/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.serialization;

import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.Cluster;
import akka.testkit.javadsl.TestKit;
import org.junit.Test;
import static org.junit.Assert.*;

// #imports
import akka.actor.*;
import akka.serialization.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

// #imports

public class SerializationDocTest {
  public
  // #my-own-serializer
  static class MyOwnSerializer extends JSerializer {

    // If you need logging here, introduce a constructor that takes an ExtendedActorSystem.
    // public MyOwnSerializer(ExtendedActorSystem actorSystem)
    // Get a logger using:
    // private final LoggingAdapter logger = Logging.getLogger(actorSystem, this);

    // This is whether "fromBinary" requires a "clazz" or not
    @Override
    public boolean includeManifest() {
      return false;
    }

    // Pick a unique identifier for your Serializer,
    // you've got a couple of billions to choose from,
    // 0 - 40 is reserved by Akka itself
    @Override
    public int identifier() {
      return 1234567;
    }

    // "toBinary" serializes the given object to an Array of Bytes
    @Override
    public byte[] toBinary(Object obj) {
      // Put the code that serializes the object here
      // #...
      return new byte[0];
      // #...
    }

    // "fromBinary" deserializes the given array,
    // using the type hint (if any, see "includeManifest" above)
    @Override
    public Object fromBinaryJava(byte[] bytes, Class<?> clazz) {
      // Put your code that deserializes here
      // #...
      return null;
      // #...
    }
  }
  // #my-own-serializer

  static class Customer {
    public final String name;

    Customer(String name) {
      this.name = name;
    }
  }

  static class User {
    public final String name;

    User(String name) {
      this.name = name;
    }
  }

  public
  // #my-own-serializer2
  static class MyOwnSerializer2 extends SerializerWithStringManifest {

    private static final String CUSTOMER_MANIFEST = "customer";
    private static final String USER_MANIFEST = "user";
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    // Pick a unique identifier for your Serializer,
    // you've got a couple of billions to choose from,
    // 0 - 40 is reserved by Akka itself
    @Override
    public int identifier() {
      return 1234567;
    }

    @Override
    public String manifest(Object obj) {
      if (obj instanceof Customer) return CUSTOMER_MANIFEST;
      else if (obj instanceof User) return USER_MANIFEST;
      else throw new IllegalArgumentException("Unknown type: " + obj);
    }

    // "toBinary" serializes the given object to an Array of Bytes
    @Override
    public byte[] toBinary(Object obj) {
      // Put the real code that serializes the object here
      if (obj instanceof Customer) return ((Customer) obj).name.getBytes(UTF_8);
      else if (obj instanceof User) return ((User) obj).name.getBytes(UTF_8);
      else throw new IllegalArgumentException("Unknown type: " + obj);
    }

    // "fromBinary" deserializes the given array,
    // using the type hint
    @Override
    public Object fromBinary(byte[] bytes, String manifest) {
      // Put the real code that deserializes here
      if (manifest.equals(CUSTOMER_MANIFEST)) return new Customer(new String(bytes, UTF_8));
      else if (manifest.equals(USER_MANIFEST)) return new User(new String(bytes, UTF_8));
      else throw new IllegalArgumentException("Unknown manifest: " + manifest);
    }
  }
  // #my-own-serializer2

  @Test
  public void serializeActorRefs() {
    final ExtendedActorSystem extendedSystem = (ExtendedActorSystem) ActorSystem.create("whatever");
    final ActorRef theActorRef = extendedSystem.deadLetters(); // Of course this should be you

    // #actorref-serializer
    // Serialize
    // (beneath toBinary)
    String serializedRef = Serialization.serializedActorPath(theActorRef);

    // Then just serialize the identifier however you like

    // Deserialize
    // (beneath fromBinary)
    final ActorRef deserializedRef = extendedSystem.provider().resolveActorRef(serializedRef);
    // Then just use the ActorRef
    // #actorref-serializer
    TestKit.shutdownActorSystem(extendedSystem);
  }

  public void demonstrateDefaultAddress() {
    // this is not meant to be run, only to be compiled
    final ActorSystem system = ActorSystem.create();
    final ActorRef theActorRef = system.deadLetters();
    // #external-address-default
    Address selfAddress = Cluster.get(system).selfAddress();

    String serializedRef = theActorRef.path().toSerializationFormatWithAddress(selfAddress);
    // #external-address-default
  }

  @Test
  public void demonstrateTheProgrammaticAPI() {
    // #programmatic
    ActorSystem system = ActorSystem.create("example");

    // Get the Serialization Extension
    Serialization serialization = SerializationExtension.get(system);

    // Have something to serialize
    String original = "woohoo";

    // Turn it into bytes, and retrieve the serializerId and manifest, which are needed for
    // deserialization
    byte[] bytes = serialization.serialize(original).get();
    int serializerId = serialization.findSerializerFor(original).identifier();
    String manifest = Serializers.manifestFor(serialization.findSerializerFor(original), original);

    // Turn it back into an object
    String back = (String) serialization.deserialize(bytes, serializerId, manifest).get();
    // #programmatic

    // Voilá!
    assertEquals(original, back);

    TestKit.shutdownActorSystem(system);
  }

  public void demonstrateTheProgrammaticAPITyped() {
    // #programmatic-typed
    akka.actor.typed.ActorSystem<Void> system =
        akka.actor.typed.ActorSystem.create(Behaviors.empty(), "example");

    // Get the Serialization Extension
    Serialization serialization = SerializationExtension.get(system);
    // #programmatic-typed
  }
}
