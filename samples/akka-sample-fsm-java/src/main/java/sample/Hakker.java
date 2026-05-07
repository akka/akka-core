package sample;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;

import java.time.Duration;

/*
 * A hakker is an awesome dude or dudette who either thinks about hacking or has to eat ;-)
 */
class Hakker {

   interface Command {}

   enum Eat implements Command {
      INSTANCE
   }

   enum Think implements Command {
      INSTANCE
   }

   static class HandleChopstickAnswer implements Command {
      final Chopstick.Answer msg;

      public HandleChopstickAnswer(Chopstick.Answer msg) {
         this.msg = msg;
      }
   }

   public static Behavior<Command> create(String name, ActorRef<Chopstick.Command> left, ActorRef<Chopstick.Command> right) {
      return Behaviors.setup(ctx -> new Hakker(ctx, name, left, right).waiting());
   }


   private final ActorContext<Command> ctx;
   private final String name;
   private final ActorRef<Chopstick.Command> left;
   private final ActorRef<Chopstick.Command> right;
   private final ActorRef<Chopstick.Answer> adapter;

   private Hakker(ActorContext<Command> ctx, String name, ActorRef<Chopstick.Command> left, ActorRef<Chopstick.Command> right) {
      this.ctx = ctx;
      this.name = name;
      this.left = left;
      this.right = right;
      this.adapter = ctx.messageAdapter(Chopstick.Answer.class, HandleChopstickAnswer::new);
   }

   private Behavior<Command> waiting() {
      return Behaviors.receive(Command.class)
         .onMessage(Think.class, msg -> {
            ctx.getLog().info("{} starts to think", name);
            return startThinking(Duration.ofSeconds(5));
         })
         .build();
   }

   //When a hakker is thinking it can become hungry
   //and try to pick up its chopsticks and eat
   private Behavior<Command> thinking() {
      return Behaviors.receive(Command.class)
         .onMessageEquals(Eat.INSTANCE, () -> {
            left.tell(new Chopstick.Take(adapter));
            right.tell(new Chopstick.Take(adapter));
            return hungry();
         })
         .build();
   }

   //When a hakker is hungry it tries to pick up its chopsticks and eat
   //When it picks one up, it goes into wait for the other
   //If the hakkers first attempt at grabbing a chopstick fails,
   //it starts to wait for the response of the other grab
   private Behavior<Command> hungry() {
      return Behaviors.receive(Command.class)
         .onMessage(HandleChopstickAnswer.class, m -> m.msg.isBusy(), (msg) ->
            firstChopstickDenied()
         )
         .onMessage(HandleChopstickAnswer.class, m -> m.msg.chopstick.equals(left), (msg) ->
            waitForOtherChopstick(right, left)
         )
         .onMessage(HandleChopstickAnswer.class, m -> m.msg.chopstick.equals(right), (msg) ->
            waitForOtherChopstick(left, right)
         )
         .build();
   }

   //When a hakker is waiting for the last chopstick it can either obtain it
   //and start eating, or the other chopstick was busy, and the hakker goes
   //back to think about how he should obtain his chopsticks :-)
   private Behavior<Command> waitForOtherChopstick(ActorRef<Chopstick.Command> chopstickToWaitFor,
                                                   ActorRef<Chopstick.Command> takenChopstick) {
      return Behaviors.receive(Command.class)
          .onMessage(HandleChopstickAnswer.class, m -> m.msg.isTaken() & m.msg.chopstick.equals(chopstickToWaitFor), msg -> {
            ctx.getLog().info("{} has picked up {} and{} and starts to eat",
               name, left.path().name(), right.path().name());
            return startEating(ctx, Duration.ofSeconds(5));
         })
         .onMessage(HandleChopstickAnswer.class, m -> m.msg.isBusy() && m.msg.chopstick.equals(chopstickToWaitFor), msg -> {
            takenChopstick.tell(new Chopstick.Put(adapter));
            return startThinking(Duration.ofMillis(10));
         })
         .build();
   }

   //When a hakker is eating, he can decide to start to think,
   //then he puts down his chopsticks and starts to think
   private Behavior<Command> eating() {
      return Behaviors.receive(Command.class)
         .onMessageEquals(Think.INSTANCE, () -> {
            ctx.getLog().info("{} puts down his chopsticks and starts to think", name);
            left.tell(new Chopstick.Put(adapter));
            right.tell(new Chopstick.Put(adapter));
            return startThinking(Duration.ofSeconds(5));
         }).build();
   }

   //When the results of the other grab comes back,
   //he needs to put it back if he got the other one.
   //Then go back and think and try to grab the chopsticks again
   private Behavior<Command> firstChopstickDenied() {
      return Behaviors.receive(Command.class)
         .onMessage(HandleChopstickAnswer.class, m -> m.msg.isTaken(), msg -> {
            msg.msg.chopstick.tell(new Chopstick.Put(adapter));
            return startThinking(Duration.ofMillis(10));
         })
         .onMessage(HandleChopstickAnswer.class, m -> m.msg.isBusy(), (msg) ->
            startThinking(Duration.ofMillis(10))
         )
         .build();
   }

   private Behavior<Command> startThinking(Duration duration) {
      ctx.scheduleOnce(duration, ctx.getSelf(), Eat.INSTANCE);
      return thinking();
   }

   private Behavior<Command> startEating(ActorContext<Command> ctx, Duration duration) {
      ctx.scheduleOnce(duration, ctx.getSelf(), Think.INSTANCE);
      return eating();
   }
}
