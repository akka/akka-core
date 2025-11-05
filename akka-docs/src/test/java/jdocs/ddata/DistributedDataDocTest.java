/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.ddata;

import static akka.cluster.ddata.Replicator.*;
import static org.junit.Assert.assertEquals;

import akka.actor.*;
import akka.cluster.ddata.*;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.ConfigFactory;
import docs.ddata.DistributedDataDocSpec;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings({"unchecked", "unused"})
public class DistributedDataDocTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system =
        ActorSystem.create(
            "DistributedDataDocTest", ConfigFactory.parseString(DistributedDataDocSpec.config()));
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  static
  // #update
  class DemonstrateUpdate extends AbstractActor {
    final SelfUniqueAddress node =
        DistributedData.get(getContext().getSystem()).selfUniqueAddress();
    final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();

    final Key<PNCounter> counter1Key = PNCounterKey.create("counter1");
    final Key<GSet<String>> set1Key = GSetKey.create("set1");
    final Key<ORSet<String>> set2Key = ORSetKey.create("set2");
    final Key<Flag> activeFlagKey = FlagKey.create("active");

    @Override
    public Receive createReceive() {
      ReceiveBuilder b = receiveBuilder();

      b.matchEquals(
          "demonstrate update",
          msg -> {
            replicator.tell(
                new Replicator.Update<PNCounter>(
                    counter1Key,
                    PNCounter.create(),
                    Replicator.writeLocal(),
                    curr -> curr.increment(node, 1)),
                getSelf());

            final WriteConsistency writeTo3 = new WriteTo(3, Duration.ofSeconds(1));
            replicator.tell(
                new Replicator.Update<GSet<String>>(
                    set1Key, GSet.create(), writeTo3, curr -> curr.add("hello")),
                getSelf());

            final WriteConsistency writeMajority = new WriteMajority(Duration.ofSeconds(5));
            replicator.tell(
                new Replicator.Update<ORSet<String>>(
                    set2Key, ORSet.create(), writeMajority, curr -> curr.add(node, "hello")),
                getSelf());

            final WriteConsistency writeAll = new WriteAll(Duration.ofSeconds(5));
            replicator.tell(
                new Replicator.Update<Flag>(
                    activeFlagKey, Flag.create(), writeAll, curr -> curr.switchOn()),
                getSelf());
          });
      // #update

      // #update-response1
      b.match(
          UpdateSuccess.class,
          a -> a.key().equals(counter1Key),
          a -> {
            // ok
          });
      // #update-response1

      // #update-response2
      b.match(
              UpdateSuccess.class,
              a -> a.key().equals(set1Key),
              a -> {
                // ok
              })
          .match(
              UpdateTimeout.class,
              a -> a.key().equals(set1Key),
              a -> {
                // write to 3 nodes failed within 1.second
              });
      // #update-response2

      // #update
      return b.build();
    }
  }
  // #update

  static
  // #update-request-context
  class DemonstrateUpdateWithRequestContext extends AbstractActor {
    final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
    final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();

    final WriteConsistency writeTwo = new WriteTo(2, Duration.ofSeconds(3));
    final Key<PNCounter> counter1Key = PNCounterKey.create("counter1");

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              String.class,
              a -> a.equals("increment"),
              a -> {
                // incoming command to increase the counter
                Optional<Object> reqContext = Optional.of(getSender());
                Replicator.Update<PNCounter> upd =
                    new Replicator.Update<PNCounter>(
                        counter1Key,
                        PNCounter.create(),
                        writeTwo,
                        reqContext,
                        curr -> curr.increment(node, 1));
                replicator.tell(upd, getSelf());
              })
          .match(
              UpdateSuccess.class,
              a -> a.key().equals(counter1Key),
              a -> {
                ActorRef replyTo = (ActorRef) a.getRequest().get();
                replyTo.tell("ack", getSelf());
              })
          .match(
              UpdateTimeout.class,
              a -> a.key().equals(counter1Key),
              a -> {
                ActorRef replyTo = (ActorRef) a.getRequest().get();
                replyTo.tell("nack", getSelf());
              })
          .build();
    }
  }
  // #update-request-context

  static
  // #get
  class DemonstrateGet extends AbstractActor {
    final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();

    final Key<PNCounter> counter1Key = PNCounterKey.create("counter1");
    final Key<GSet<String>> set1Key = GSetKey.create("set1");
    final Key<ORSet<String>> set2Key = ORSetKey.create("set2");
    final Key<Flag> activeFlagKey = FlagKey.create("active");

    @Override
    public Receive createReceive() {
      ReceiveBuilder b = receiveBuilder();

      b.matchEquals(
          "demonstrate get",
          msg -> {
            replicator.tell(
                new Replicator.Get<PNCounter>(counter1Key, Replicator.readLocal()), getSelf());

            final ReadConsistency readFrom3 = new ReadFrom(3, Duration.ofSeconds(1));
            replicator.tell(new Replicator.Get<GSet<String>>(set1Key, readFrom3), getSelf());

            final ReadConsistency readMajority = new ReadMajority(Duration.ofSeconds(5));
            replicator.tell(new Replicator.Get<ORSet<String>>(set2Key, readMajority), getSelf());

            final ReadConsistency readAll = new ReadAll(Duration.ofSeconds(5));
            replicator.tell(new Replicator.Get<Flag>(activeFlagKey, readAll), getSelf());
          });
      // #get

      // #get-response1
      b.match(
              GetSuccess.class,
              a -> a.key().equals(counter1Key),
              a -> {
                GetSuccess<PNCounter> g = a;
                BigInteger value = g.dataValue().getValue();
              })
          .match(
              NotFound.class,
              a -> a.key().equals(counter1Key),
              a -> {
                // key counter1 does not exist
              });
      // #get-response1

      // #get-response2
      b.match(
              GetSuccess.class,
              a -> a.key().equals(set1Key),
              a -> {
                GetSuccess<GSet<String>> g = a;
                Set<String> value = g.dataValue().getElements();
              })
          .match(
              GetFailure.class,
              a -> a.key().equals(set1Key),
              a -> {
                // read from 3 nodes failed within 1.second
              })
          .match(
              NotFound.class,
              a -> a.key().equals(set1Key),
              a -> {
                // key set1 does not exist
              });
      // #get-response2

      // #get
      return b.build();
    }
  }
  // #get

  static
  // #get-request-context
  class DemonstrateGetWithRequestContext extends AbstractActor {
    final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();

    final ReadConsistency readTwo = new ReadFrom(2, Duration.ofSeconds(3));
    final Key<PNCounter> counter1Key = PNCounterKey.create("counter1");

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              String.class,
              a -> a.equals("get-count"),
              a -> {
                // incoming request to retrieve current value of the counter
                Optional<Object> reqContext = Optional.of(getSender());
                replicator.tell(new Replicator.Get<PNCounter>(counter1Key, readTwo), getSelf());
              })
          .match(
              GetSuccess.class,
              a -> a.key().equals(counter1Key),
              a -> {
                ActorRef replyTo = (ActorRef) a.getRequest().get();
                GetSuccess<PNCounter> g = a;
                long value = g.dataValue().getValue().longValue();
                replyTo.tell(value, getSelf());
              })
          .match(
              GetFailure.class,
              a -> a.key().equals(counter1Key),
              a -> {
                ActorRef replyTo = (ActorRef) a.getRequest().get();
                replyTo.tell(-1L, getSelf());
              })
          .match(
              NotFound.class,
              a -> a.key().equals(counter1Key),
              a -> {
                ActorRef replyTo = (ActorRef) a.getRequest().get();
                replyTo.tell(0L, getSelf());
              })
          .build();
    }
  }
  // #get-request-context

  static
  // #subscribe
  class DemonstrateSubscribe extends AbstractActor {
    final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();
    final Key<PNCounter> counter1Key = PNCounterKey.create("counter1");

    BigInteger currentValue = BigInteger.valueOf(0);

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Changed.class,
              a -> a.key().equals(counter1Key),
              a -> {
                Changed<PNCounter> g = a;
                currentValue = g.dataValue().getValue();
              })
          .match(
              String.class,
              a -> a.equals("get-count"),
              a -> {
                // incoming request to retrieve current value of the counter
                getSender().tell(currentValue, getSender());
              })
          .build();
    }

    @Override
    public void preStart() {
      // subscribe to changes of the Counter1Key value
      replicator.tell(new Subscribe<PNCounter>(counter1Key, getSelf()), ActorRef.noSender());
    }
  }
  // #subscribe

  static
  // #delete
  class DemonstrateDelete extends AbstractActor {
    final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();

    final Key<PNCounter> counter1Key = PNCounterKey.create("counter1");
    final Key<ORSet<String>> set2Key = ORSetKey.create("set2");

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .matchEquals(
              "demonstrate delete",
              msg -> {
                replicator.tell(
                    new Delete<PNCounter>(counter1Key, Replicator.writeLocal()), getSelf());

                final WriteConsistency writeMajority = new WriteMajority(Duration.ofSeconds(5));
                replicator.tell(new Delete<PNCounter>(counter1Key, writeMajority), getSelf());
              })
          .build();
    }
  }
  // #delete

  public void demonstratePNCounter() {
    // #pncounter
    final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
    final PNCounter c0 = PNCounter.create();
    final PNCounter c1 = c0.increment(node, 1);
    final PNCounter c2 = c1.increment(node, 7);
    final PNCounter c3 = c2.decrement(node, 2);
    System.out.println(c3.value()); // 6
    // #pncounter
  }

  public void demonstratePNCounterMap() {
    // #pncountermap
    final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
    final PNCounterMap<String> m0 = PNCounterMap.create();
    final PNCounterMap<String> m1 = m0.increment(node, "a", 7);
    final PNCounterMap<String> m2 = m1.decrement(node, "a", 2);
    final PNCounterMap<String> m3 = m2.increment(node, "b", 1);
    System.out.println(m3.get("a")); // 5
    System.out.println(m3.getEntries());
    // #pncountermap
  }

  public void demonstrateGSet() {
    // #gset
    final GSet<String> s0 = GSet.create();
    final GSet<String> s1 = s0.add("a");
    final GSet<String> s2 = s1.add("b").add("c");
    if (s2.contains("a")) System.out.println(s2.getElements()); // a, b, c
    // #gset
  }

  public void demonstrateORSet() {
    // #orset
    final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
    final ORSet<String> s0 = ORSet.create();
    final ORSet<String> s1 = s0.add(node, "a");
    final ORSet<String> s2 = s1.add(node, "b");
    final ORSet<String> s3 = s2.remove(node, "a");
    System.out.println(s3.getElements()); // b
    // #orset
  }

  public void demonstrateORMultiMap() {
    // #ormultimap
    final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
    final ORMultiMap<String, Integer> m0 = ORMultiMap.create();
    final ORMultiMap<String, Integer> m1 = m0.put(node, "a", new HashSet<>(Arrays.asList(1, 2, 3)));
    final ORMultiMap<String, Integer> m2 = m1.addBinding(node, "a", 4);
    final ORMultiMap<String, Integer> m3 = m2.removeBinding(node, "a", 2);
    final ORMultiMap<String, Integer> m4 = m3.addBinding(node, "b", 1);
    System.out.println(m4.getEntries());
    // #ormultimap
  }

  public void demonstrateFlag() {
    // #flag
    final Flag f0 = Flag.create();
    final Flag f1 = f0.switchOn();
    System.out.println(f1.enabled());
    // #flag
  }

  @Test
  public void demonstrateLWWRegister() {
    // #lwwregister
    final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
    final LWWRegister<String> r1 = LWWRegister.create(node, "Hello");
    final LWWRegister<String> r2 = r1.withValue(node, "Hi");
    System.out.println(r1.value() + " by " + r1.updatedBy() + " at " + r1.timestamp());
    // #lwwregister
    assertEquals("Hi", r2.value());
  }

  static
  // #lwwregister-custom-clock
  class Record {
    public final int version;
    public final String name;
    public final String address;

    public Record(int version, String name, String address) {
      this.version = version;
      this.name = name;
      this.address = address;
    }
  }

  // #lwwregister-custom-clock

  public void demonstrateLWWRegisterWithCustomClock() {
    // #lwwregister-custom-clock

    final SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
    final LWWRegister.Clock<Record> recordClock =
        new LWWRegister.Clock<Record>() {
          @Override
          public long apply(long currentTimestamp, Record value) {
            return value.version;
          }
        };

    final Record record1 = new Record(1, "Alice", "Union Square");
    final LWWRegister<Record> r1 = LWWRegister.create(node, record1);

    final Record record2 = new Record(2, "Alice", "Madison Square");
    final LWWRegister<Record> r2 = LWWRegister.create(node, record2);

    final LWWRegister<Record> r3 = r1.merge(r2);
    System.out.println(r3.value());
    // #lwwregister-custom-clock

    assertEquals("Madison Square", r3.value().address);
  }
}
