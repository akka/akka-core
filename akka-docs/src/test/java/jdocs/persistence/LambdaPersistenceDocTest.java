/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.persistence;

import akka.actor.*;
import akka.japi.Procedure;
import akka.pattern.BackoffOpts;
import akka.pattern.BackoffSupervisor;
import akka.persistence.*;
import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public class LambdaPersistenceDocTest {

  public interface SomeOtherMessage {}

  public interface PersistentActorMethods {
    // #persistence-id
    public String persistenceId();

    // #persistence-id
  }

  public interface PersistenceActorRecoveryMethods {
    // #recovery-status
    public boolean recoveryRunning();

    public boolean recoveryFinished();
    // #recovery-status
  }

  static Object o2 =
      new Object() {
        abstract class MyPersistentActor1 extends AbstractPersistentActor {
          // #recovery-disabled
          @Override
          public Recovery recovery() {
            return Recovery.none();
          }
          // #recovery-disabled

          // #recover-on-restart-disabled
          @Override
          public void preRestart(Throwable reason, Optional<Object> message) {}
          // #recover-on-restart-disabled
        }

        abstract class MyPersistentActor2 extends AbstractPersistentActor {
          // #recovery-custom
          @Override
          public Recovery recovery() {
            return Recovery.create(457L);
          }
          // #recovery-custom
        }

        class MyPersistentActor4 extends AbstractPersistentActor implements PersistentActorMethods {
          // #persistence-id-override
          @Override
          public String persistenceId() {
            return "my-stable-persistence-id";
          }

          // #persistence-id-override

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(
                    String.class,
                    cmd -> {
                      /* ... */
                    })
                .build();
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder()
                .match(
                    String.class,
                    evt -> {
                      /* ... */
                    })
                .build();
          }
        }

        // #recovery-completed
        class MyPersistentActor5 extends AbstractPersistentActor {

          @Override
          public String persistenceId() {
            return "my-stable-persistence-id";
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder()
                .match(
                    RecoveryCompleted.class,
                    r -> {
                      // perform init after recovery, before any other messages
                      // ...
                    })
                .match(String.class, this::handleEvent)
                .build();
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(String.class, s -> s.equals("cmd"), s -> persist("evt", this::handleEvent))
                .build();
          }

          private void handleEvent(String event) {
            // update state
            // ...
          }
        }
        // #recovery-completed

        abstract class MyPersistentActor6 extends AbstractPersistentActor {
          // #recovery-no-snap
          @Override
          public Recovery recovery() {
            return Recovery.create(SnapshotSelectionCriteria.none());
          }
          // #recovery-no-snap
        }

        abstract class MyActor extends AbstractPersistentActor {
          // #backoff
          @Override
          public void preStart() throws Exception {
            final Props childProps = Props.create(MyPersistentActor1.class);
            final Props props =
                BackoffSupervisor.props(
                    BackoffOpts.onStop(
                        childProps, "myActor", Duration.ofSeconds(3), Duration.ofSeconds(30), 0.2));
            getContext().actorOf(props, "mySupervisor");
            super.preStart();
          }
          // #backoff
        }
      };

  static Object atLeastOnceExample =
      new Object() {
        // #at-least-once-example

        class Msg implements Serializable {
          private static final long serialVersionUID = 1L;
          public final long deliveryId;
          public final String s;

          public Msg(long deliveryId, String s) {
            this.deliveryId = deliveryId;
            this.s = s;
          }
        }

        class Confirm implements Serializable {
          private static final long serialVersionUID = 1L;
          public final long deliveryId;

          public Confirm(long deliveryId) {
            this.deliveryId = deliveryId;
          }
        }

        class MsgSent implements Serializable {
          private static final long serialVersionUID = 1L;
          public final String s;

          public MsgSent(String s) {
            this.s = s;
          }
        }

        class MsgConfirmed implements Serializable {
          private static final long serialVersionUID = 1L;
          public final long deliveryId;

          public MsgConfirmed(long deliveryId) {
            this.deliveryId = deliveryId;
          }
        }

        class MyPersistentActor extends AbstractPersistentActorWithAtLeastOnceDelivery {
          private final ActorSelection destination;

          public MyPersistentActor(ActorSelection destination) {
            this.destination = destination;
          }

          @Override
          public String persistenceId() {
            return "persistence-id";
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(
                    String.class,
                    s -> {
                      persist(new MsgSent(s), evt -> updateState(evt));
                    })
                .match(
                    Confirm.class,
                    confirm -> {
                      persist(new MsgConfirmed(confirm.deliveryId), evt -> updateState(evt));
                    })
                .build();
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder().match(Object.class, evt -> updateState(evt)).build();
          }

          void updateState(Object event) {
            if (event instanceof MsgSent) {
              final MsgSent evt = (MsgSent) event;
              deliver(destination, deliveryId -> new Msg(deliveryId, evt.s));
            } else if (event instanceof MsgConfirmed) {
              final MsgConfirmed evt = (MsgConfirmed) event;
              confirmDelivery(evt.deliveryId);
            }
          }
        }

        class MyDestination extends AbstractActor {
          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(
                    Msg.class,
                    msg -> {
                      // ...
                      getSender().tell(new Confirm(msg.deliveryId), getSelf());
                    })
                .build();
          }
        }
        // #at-least-once-example

      };

  static Object o4 =
      new Object() {
        class MyPersistentActor extends AbstractPersistentActor {

          private void updateState(String evt) {}

          // #save-snapshot
          private Object state;
          private int snapShotInterval = 1000;

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(
                    SaveSnapshotSuccess.class,
                    ss -> {
                      SnapshotMetadata metadata = ss.metadata();
                      // ...
                    })
                .match(
                    SaveSnapshotFailure.class,
                    sf -> {
                      SnapshotMetadata metadata = sf.metadata();
                      // ...
                    })
                .match(
                    String.class,
                    cmd -> {
                      persist(
                          "evt-" + cmd,
                          e -> {
                            updateState(e);
                            if (lastSequenceNr() % snapShotInterval == 0 && lastSequenceNr() != 0)
                              saveSnapshot(state);
                          });
                    })
                .build();
          }
          // #save-snapshot

          @Override
          public String persistenceId() {
            return "persistence-id";
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder()
                .match(
                    RecoveryCompleted.class,
                    r -> {
                      /* ...*/
                    })
                .build();
          }
        }
      };

  static Object o5 =
      new Object() {

        class MyPersistentActor extends AbstractPersistentActor {

          // #snapshot-criteria
          @Override
          public Recovery recovery() {
            return Recovery.create(
                SnapshotSelectionCriteria.create(457L, System.currentTimeMillis()));
          }
          // #snapshot-criteria

          // #snapshot-offer
          private Object state;

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder()
                .match(
                    SnapshotOffer.class,
                    s -> {
                      state = s.snapshot();
                      // ...
                    })
                .match(
                    String.class,
                    s -> {
                      /* ...*/
                    })
                .build();
          }
          // #snapshot-offer

          @Override
          public String persistenceId() {
            return "persistence-id";
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(
                    String.class,
                    s -> {
                      /* ...*/
                    })
                .build();
          }
        }

        class MyActor extends AbstractActor {
          private final ActorRef persistentActor =
              getContext().actorOf(Props.create(MyPersistentActor.class));

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(
                    Object.class,
                    o -> {
                      /* ... */
                    })
                .build();
          }
        }
      };

  static Object o9 =
      new Object() {
        // #persist-async
        class MyPersistentActor extends AbstractPersistentActor {

          @Override
          public String persistenceId() {
            return "my-stable-persistence-id";
          }

          private void handleCommand(String c) {
            getSender().tell(c, getSelf());

            persistAsync(
                String.format("evt-%s-1", c),
                e -> {
                  getSender().tell(e, getSelf());
                });
            persistAsync(
                String.format("evt-%s-2", c),
                e -> {
                  getSender().tell(e, getSelf());
                });
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder().match(String.class, this::handleCommand).build();
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder().match(String.class, this::handleCommand).build();
          }
        }
        // #persist-async

        public void usage() {
          final ActorSystem system = ActorSystem.create("example");
          // #persist-async-usage
          final ActorRef persistentActor = system.actorOf(Props.create(MyPersistentActor.class));
          persistentActor.tell("a", null);
          persistentActor.tell("b", null);

          // possible order of received messages:
          // a
          // b
          // evt-a-1
          // evt-a-2
          // evt-b-1
          // evt-b-2
          // #persist-async-usage
        }
      };

  static Object o10 =
      new Object() {
        // #defer
        class MyPersistentActor extends AbstractPersistentActor {

          @Override
          public String persistenceId() {
            return "my-stable-persistence-id";
          }

          private void handleCommand(String c) {
            persistAsync(
                String.format("evt-%s-1", c),
                e -> {
                  getSender().tell(e, getSelf());
                });
            persistAsync(
                String.format("evt-%s-2", c),
                e -> {
                  getSender().tell(e, getSelf());
                });

            deferAsync(
                String.format("evt-%s-3", c),
                e -> {
                  getSender().tell(e, getSelf());
                });
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder().match(String.class, this::handleCommand).build();
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder().match(String.class, this::handleCommand).build();
          }
        }
        // #defer

        public void usage() {
          final ActorSystem system = ActorSystem.create("example");
          final ActorRef sender = null; // your imaginary sender here
          // #defer-caller
          final ActorRef persistentActor = system.actorOf(Props.create(MyPersistentActor.class));
          persistentActor.tell("a", sender);
          persistentActor.tell("b", sender);

          // order of received messages:
          // a
          // b
          // evt-a-1
          // evt-a-2
          // evt-a-3
          // evt-b-1
          // evt-b-2
          // evt-b-3
          // #defer-caller
        }
      };

  static Object o100 =
      new Object() {
        // #defer-with-persist
        class MyPersistentActor extends AbstractPersistentActor {

          @Override
          public String persistenceId() {
            return "my-stable-persistence-id";
          }

          private void handleCommand(String c) {
            persist(
                String.format("evt-%s-1", c),
                e -> {
                  sender().tell(e, self());
                });
            persist(
                String.format("evt-%s-2", c),
                e -> {
                  sender().tell(e, self());
                });

            defer(
                String.format("evt-%s-3", c),
                e -> {
                  sender().tell(e, self());
                });
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder().match(String.class, this::handleCommand).build();
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder().match(String.class, this::handleCommand).build();
          }
        }
        // #defer-with-persist

      };

  static Object o11 =
      new Object() {

        class MyPersistentActor extends AbstractPersistentActor {
          @Override
          public String persistenceId() {
            return "my-stable-persistence-id";
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder().matchAny(event -> {}).build();
          }

          // #nested-persist-persist
          @Override
          public Receive createReceiveRecover() {
            final Procedure<String> replyToSender = event -> getSender().tell(event, getSelf());

            return receiveBuilder()
                .match(
                    String.class,
                    msg -> {
                      persist(
                          String.format("%s-outer-1", msg),
                          event -> {
                            getSender().tell(event, getSelf());
                            persist(String.format("%s-inner-1", event), replyToSender);
                          });

                      persist(
                          String.format("%s-outer-2", msg),
                          event -> {
                            getSender().tell(event, getSelf());
                            persist(String.format("%s-inner-2", event), replyToSender);
                          });
                    })
                .build();
          }
          // #nested-persist-persist

          void usage(ActorRef persistentActor) {
            // #nested-persist-persist-caller
            persistentActor.tell("a", ActorRef.noSender());
            persistentActor.tell("b", ActorRef.noSender());

            // order of received messages:
            // a
            // a-outer-1
            // a-outer-2
            // a-inner-1
            // a-inner-2
            // and only then process "b"
            // b
            // b-outer-1
            // b-outer-2
            // b-inner-1
            // b-inner-2

            // #nested-persist-persist-caller
          }
        }

        class MyPersistAsyncActor extends AbstractPersistentActor {
          @Override
          public String persistenceId() {
            return "my-stable-persistence-id";
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder().matchAny(event -> {}).build();
          }

          // #nested-persistAsync-persistAsync
          @Override
          public Receive createReceive() {
            final Procedure<String> replyToSender = event -> getSender().tell(event, getSelf());

            return receiveBuilder()
                .match(
                    String.class,
                    msg -> {
                      persistAsync(
                          String.format("%s-outer-1", msg),
                          event -> {
                            getSender().tell(event, getSelf());
                            persistAsync(String.format("%s-inner-1", event), replyToSender);
                          });

                      persistAsync(
                          String.format("%s-outer-2", msg),
                          event -> {
                            getSender().tell(event, getSelf());
                            persistAsync(String.format("%s-inner-1", event), replyToSender);
                          });
                    })
                .build();
          }
          // #nested-persistAsync-persistAsync

          void usage(ActorRef persistentActor) {
            // #nested-persistAsync-persistAsync-caller
            persistentActor.tell("a", getSelf());
            persistentActor.tell("b", getSelf());

            // order of received messages:
            // a
            // b
            // a-outer-1
            // a-outer-2
            // b-outer-1
            // b-outer-2
            // a-inner-1
            // a-inner-2
            // b-inner-1
            // b-inner-2

            // which can be seen as the following causal relationship:
            // a -> a-outer-1 -> a-outer-2 -> a-inner-1 -> a-inner-2
            // b -> b-outer-1 -> b-outer-2 -> b-inner-1 -> b-inner-2

            // #nested-persistAsync-persistAsync-caller
          }
        }
      };

  static Object o14 =
      new Object() {
        // #safe-shutdown
        final class Shutdown {}

        class MyPersistentActor extends AbstractPersistentActor {
          @Override
          public String persistenceId() {
            return "some-persistence-id";
          }

          @Override
          public Receive createReceive() {
            return receiveBuilder()
                .match(
                    Shutdown.class,
                    shutdown -> {
                      getContext().stop(getSelf());
                    })
                .match(
                    String.class,
                    msg -> {
                      System.out.println(msg);
                      persist("handle-" + msg, e -> System.out.println(e));
                    })
                .build();
          }

          @Override
          public Receive createReceiveRecover() {
            return receiveBuilder().matchAny(any -> {}).build();
          }
        }
        // #safe-shutdown

        public void usage() {
          final ActorSystem system = ActorSystem.create("example");
          final ActorRef persistentActor = system.actorOf(Props.create(MyPersistentActor.class));
          // #safe-shutdown-example-bad
          // UN-SAFE, due to PersistentActor's command stashing:
          persistentActor.tell("a", ActorRef.noSender());
          persistentActor.tell("b", ActorRef.noSender());
          persistentActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
          // order of received messages:
          // a
          //   # b arrives at mailbox, stashing;        internal-stash = [b]
          //   # PoisonPill arrives at mailbox, stashing; internal-stash = [b, Shutdown]
          // PoisonPill is an AutoReceivedMessage, is handled automatically
          // !! stop !!
          // Actor is stopped without handling `b` nor the `a` handler!
          // #safe-shutdown-example-bad

          // #safe-shutdown-example-good
          // SAFE:
          persistentActor.tell("a", ActorRef.noSender());
          persistentActor.tell("b", ActorRef.noSender());
          persistentActor.tell(new Shutdown(), ActorRef.noSender());
          // order of received messages:
          // a
          //   # b arrives at mailbox, stashing;        internal-stash = [b]
          //   # Shutdown arrives at mailbox, stashing; internal-stash = [b, Shutdown]
          // handle-a
          //   # unstashing;                            internal-stash = [Shutdown]
          // b
          // handle-b
          //   # unstashing;                            internal-stash = []
          // Shutdown
          // -- stop --
          // #safe-shutdown-example-good
        }
      };
}
