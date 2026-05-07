package sample.cluster.stats

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import sample.cluster.CborSerializable

import scala.concurrent.duration._

//#worker
object StatsWorker {

  trait Command
  final case class Process(word: String, replyTo: ActorRef[Processed])
      extends Command
      with CborSerializable
  private case object EvictCache extends Command

  final case class Processed(word: String, length: Int) extends CborSerializable

  def apply(): Behavior[Command] = Behaviors.setup { ctx =>
    Behaviors.withTimers { timers =>
      ctx.log.info("Worker starting up")
      timers.startTimerWithFixedDelay(EvictCache, EvictCache, 30.seconds)

      withCache(ctx, Map.empty)
    }
  }

  private def withCache(ctx: ActorContext[Command],
                        cache: Map[String, Int]): Behavior[Command] =
    Behaviors.receiveMessage {
      case Process(word, replyTo) =>
        ctx.log.info("Worker processing request [{}]", word)
        cache.get(word) match {
          case Some(length) =>
            replyTo ! Processed(word, length)
            Behaviors.same
          case None =>
            val length = word.length
            val updatedCache = cache + (word -> length)
            replyTo ! Processed(word, length)
            withCache(ctx, updatedCache)
        }
      case EvictCache =>
        withCache(ctx, Map.empty)
    }
}
//#worker
