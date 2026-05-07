/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.CountDownLatch

import scala.collection.mutable
import scala.util.Random

object RequestResponseActors {

  case class Request(userId: Int)
  case class User(userId: Int, firstName: String, lastName: String, ssn: Int, friends: Seq[Int])

  class UserQueryActor(latch: CountDownLatch, numQueries: Int, numUsersInDB: Int) extends Actor {

    private var left = numQueries
    private val receivedUsers: mutable.Map[Int, User] = mutable.Map()
    private val randGenerator = new Random()

    override def receive: Receive = {
      case u: User => {
        receivedUsers.put(u.userId, u)
        if (left == 0) {
          latch.countDown()
          context.stop(self)
        } else {
          sender() ! Request(randGenerator.nextInt(numUsersInDB))
        }
        left -= 1
      }
    }
  }

  object UserQueryActor {
    def props(latch: CountDownLatch, numQueries: Int, numUsersInDB: Int) = {
      Props(new UserQueryActor(latch, numQueries, numUsersInDB))
    }
  }

  class UserServiceActor(userDb: Map[Int, User], latch: CountDownLatch, numQueries: Int) extends Actor {
    private var left = numQueries
    def receive = {
      case Request(id) =>
        userDb.get(id) match {
          case Some(u) => sender() ! u
          case None    =>
        }
        if (left == 0) {
          latch.countDown()
          context.stop(self)
        }
        left -= 1
    }

  }

  object UserServiceActor {
    def props(latch: CountDownLatch, numQueries: Int, numUsersInDB: Int) = {
      val r = new Random()
      val users = for {
        id <- 0 until numUsersInDB
        firstName = r.nextString(5)
        lastName = r.nextString(7)
        ssn = r.nextInt()
        friendIds = for { _ <- 0 until 5 } yield r.nextInt(numUsersInDB)
      } yield id -> User(id, firstName, lastName, ssn, friendIds)
      Props(new UserServiceActor(users.toMap, latch, numQueries))
    }
  }

  def startUserQueryActorPairs(numActors: Int, numQueriesPerActor: Int, numUsersInDBPerActor: Int, dispatcher: String)(
      implicit system: ActorSystem) = {
    val fullPathToDispatcher = "akka.actor." + dispatcher
    val latch = new CountDownLatch(numActors)
    val actorsPairs = for {
      _ <- (1 to (numActors / 2)).toVector
      userQueryActor = system.actorOf(
        UserQueryActor.props(latch, numQueriesPerActor, numUsersInDBPerActor).withDispatcher(fullPathToDispatcher))
      userServiceActor = system.actorOf(
        UserServiceActor.props(latch, numQueriesPerActor, numUsersInDBPerActor).withDispatcher(fullPathToDispatcher))
    } yield (userQueryActor, userServiceActor)
    (actorsPairs, latch)
  }

  def initiateQuerySimulation(requestResponseActorPairs: Seq[(ActorRef, ActorRef)], inFlight: Int) = {
    for {
      (queryActor, serviceActor) <- requestResponseActorPairs
      i <- 1 to inFlight
    } {
      serviceActor.tell(Request(i), queryActor)
    }
  }

}
