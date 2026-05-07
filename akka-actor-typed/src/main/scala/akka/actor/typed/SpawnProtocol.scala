/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.annotation.tailrec

import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.DoNotInherit

/**
 * A message protocol for actors that support spawning a child actor when receiving a [[SpawnProtocol#Spawn]]
 * message and sending back the [[ActorRef]] of the child actor. Create instances through the [[SpawnProtocol#apply]]
 * or [[SpawnProtocol#create]] factory methods.
 *
 * The typical usage of this is to use it as the guardian actor of the [[ActorSystem]], possibly combined with
 * `Behaviors.setup` to starts some initial tasks or actors. Child actors can then be started from the outside
 * by telling or asking [[SpawnProtocol#Spawn]] to the actor reference of the system. When using `ask` this is
 * similar to how [[akka.actor.ActorSystem#actorOf]] can be used in classic actors with the difference that
 * a `Future` / `CompletionStage` of the `ActorRef` is returned.
 *
 * Stopping children is done through specific support in the protocol of the children, or stopping the entire
 * spawn protocol actor.
 */
object SpawnProtocol {

  /**
   * Not for user extension
   */
  @DoNotInherit sealed trait Command

  /**
   * Spawn a child actor with the given `behavior` and send back the `ActorRef` of that child to the given
   * `replyTo` destination.
   *
   * If `name` is an empty string an anonymous actor (with automatically generated name) will be created.
   *
   * If the `name` is already taken of an existing actor a unique name will be used by appending a suffix
   * to the the `name`. The exact format or value of the suffix is an implementation detail that is
   * undefined. This means that reusing the same name for several actors will not result in
   * `InvalidActorNameException`, but it's better to use unique names to begin with.
   */
  final case class Spawn[T](behavior: Behavior[T], name: String, props: Props, replyTo: ActorRef[ActorRef[T]])
      extends Command

  /**
   * Java API: returns a behavior that can be commanded to spawn arbitrary children.
   */
  def create(): Behavior[Command] = apply()

  /**
   * Scala API: returns a behavior that can be commanded to spawn arbitrary children.
   */
  def apply(): Behavior[Command] =
    Behaviors.receive { (ctx, msg) =>
      msg match {
        case Spawn(bhvr: Behavior[t], name, props, replyTo) =>
          val ref =
            if (name == null || name.equals(""))
              ctx.spawnAnonymous(bhvr, props)
            else {

              @tailrec def spawnWithUniqueName(c: Int): ActorRef[t] = {
                val nameSuggestion = if (c == 0) name else s"$name-$c"
                ctx.child(nameSuggestion) match {
                  case Some(_) => spawnWithUniqueName(c + 1) // already taken, try next
                  case None    => ctx.spawn(bhvr, nameSuggestion, props)
                }
              }

              spawnWithUniqueName(0)
            }
          replyTo ! ref
          Behaviors.same
      }
    }

}
