package sample

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import sample.Chopstick.Busy
import sample.Chopstick.ChopstickAnswer
import sample.Chopstick.ChopstickMessage
import sample.Chopstick.Put
import sample.Chopstick.Take
import sample.Chopstick.Taken
import sample.Hakker.Command

import scala.concurrent.duration._

// Akka adaptation of
// http://www.dalnefre.com/wp/2010/08/dining-philosophers-in-humus/

/*
 * A Chopstick is an actor, it can be taken, and put back
 */
object Chopstick {
  sealed trait ChopstickMessage
  final case class Take(ref: ActorRef[ChopstickAnswer]) extends ChopstickMessage
  final case class Put(ref: ActorRef[ChopstickAnswer]) extends ChopstickMessage

  sealed trait ChopstickAnswer
  final case class Taken(chopstick: ActorRef[ChopstickMessage]) extends ChopstickAnswer
  final case class Busy(chopstick: ActorRef[ChopstickMessage]) extends ChopstickAnswer

  def apply(): Behavior[ChopstickMessage] = available()

  //When a Chopstick is taken by a hakker
  //It will refuse to be taken by other hakkers
  //But the owning hakker can put it back
  private def takenBy(hakker: ActorRef[ChopstickAnswer]): Behavior[ChopstickMessage] = {
    Behaviors.receive {
      case (ctx, Take(otherHakker)) =>
        otherHakker ! Busy(ctx.self)
        Behaviors.same
      case (_, Put(`hakker`)) =>
        available()
      case _ =>
        // here and below it's left to be explicit about partial definition,
        // but can be omitted when Behaviors.receiveMessagePartial is in use
        Behaviors.unhandled
    }
  }

  //When a Chopstick is available, it can be taken by a hakker
  private def available(): Behavior[ChopstickMessage] = {
    Behaviors.receivePartial {
      case (ctx, Take(hakker)) =>
        hakker ! Taken(ctx.self)
        takenBy(hakker)
    }
  }
}

/*
 * A hakker is an awesome dude or dudette who either thinks about hacking or has to eat ;-)
 */
object Hakker {
  sealed trait Command
  case object Think extends Command
  case object Eat extends Command
  final case class HandleChopstickAnswer(msg: ChopstickAnswer) extends Command

  def apply(name: String, left: ActorRef[ChopstickMessage], right: ActorRef[ChopstickMessage]): Behavior[Command] =
    Behaviors.setup { ctx =>
      new Hakker(ctx, name, left, right).waiting
    }
}

class Hakker(
    ctx: ActorContext[Command],
    name: String,
    left: ActorRef[ChopstickMessage],
    right: ActorRef[ChopstickMessage]) {

  import Hakker._

  private val adapter = ctx.messageAdapter(HandleChopstickAnswer.apply)

  val waiting: Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case Think =>
        ctx.log.info("{} starts to think", name)
        startThinking(ctx, 5.seconds)
    }

  //When a hakker is thinking it can become hungry
  //and try to pick up its chopsticks and eat
  private val thinking: Behavior[Command] = {
    Behaviors.receiveMessagePartial {
      case Eat =>
        left ! Chopstick.Take(adapter)
        right ! Chopstick.Take(adapter)
        hungry
    }
  }

  //When a hakker is hungry it tries to pick up its chopsticks and eat
  //When it picks one up, it goes into wait for the other
  //If the hakkers first attempt at grabbing a chopstick fails,
  //it starts to wait for the response of the other grab
  private lazy val hungry: Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case HandleChopstickAnswer(Taken(`left`)) =>
        waitForOtherChopstick(chopstickToWaitFor = right, takenChopstick = left)

      case HandleChopstickAnswer(Taken(`right`)) =>
        waitForOtherChopstick(chopstickToWaitFor = left, takenChopstick = right)

      case HandleChopstickAnswer(Busy(_)) =>
        firstChopstickDenied
    }

  //When a hakker is waiting for the last chopstick it can either obtain it
  //and start eating, or the other chopstick was busy, and the hakker goes
  //back to think about how he should obtain his chopsticks :-)
  private def waitForOtherChopstick(
      chopstickToWaitFor: ActorRef[ChopstickMessage],
      takenChopstick: ActorRef[ChopstickMessage]): Behavior[Command] = {
    Behaviors.receiveMessagePartial {
      case HandleChopstickAnswer(Taken(`chopstickToWaitFor`)) =>
        ctx.log.info("{} has picked up {} and {} and starts to eat", name, left.path.name, right.path.name)
        startEating(ctx, 5.seconds)

      case HandleChopstickAnswer(Busy(`chopstickToWaitFor`)) =>
        takenChopstick ! Put(adapter)
        startThinking(ctx, 10.milliseconds)
    }
  }

  //When a hakker is eating, he can decide to start to think,
  //then he puts down his chopsticks and starts to think
  private lazy val eating: Behavior[Command] = {
    Behaviors.receiveMessagePartial {
      case Think =>
        ctx.log.info("{} puts down his chopsticks and starts to think", name)
        left ! Put(adapter)
        right ! Put(adapter)
        startThinking(ctx, 5.seconds)
    }
  }

  //When the results of the other grab comes back,
  //he needs to put it back if he got the other one.
  //Then go back and think and try to grab the chopsticks again
  private lazy val firstChopstickDenied: Behavior[Command] = {
    Behaviors.receiveMessagePartial {
      case HandleChopstickAnswer(Taken(chopstick)) =>
        chopstick ! Put(adapter)
        startThinking(ctx, 10.milliseconds)
      case HandleChopstickAnswer(Busy(_)) =>
        startThinking(ctx, 10.milliseconds)
    }
  }

  private def startThinking(ctx: ActorContext[Command], duration: FiniteDuration) = {
    Behaviors.withTimers[Command] { timers =>
      timers.startSingleTimer(Eat, Eat, duration)
      thinking
    }
  }

  private def startEating(ctx: ActorContext[Command], duration: FiniteDuration) = {
    Behaviors.withTimers[Command] { timers =>
      timers.startSingleTimer(Think, Think, duration)
      eating
    }
  }
}

object DiningHakkers {

  def apply(): Behavior[NotUsed] = Behaviors.setup { context =>
    //Create 5 chopsticks
    val chopsticks =
      for (i <- 1 to 5)
        yield context.spawn(Chopstick(), "Chopstick" + i)

    //Create 5 awesome hakkers and assign them their left and right chopstick
    val hakkers = for {
      (name, i) <- List("Ghosh", "Boner", "Klang", "Krasser", "Manie").zipWithIndex
    } yield context.spawn(Hakker(name, chopsticks(i), chopsticks((i + 1) % 5)), name)

    //Signal all hakkers that they should start thinking, and watch the show
    hakkers.foreach(_ ! Hakker.Think)

    Behaviors.empty
  }

  def main(args: Array[String]): Unit = {
    ActorSystem(DiningHakkers(), "DiningHakkers")
  }
}
