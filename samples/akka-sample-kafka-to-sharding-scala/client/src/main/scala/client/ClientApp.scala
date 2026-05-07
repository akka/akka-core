package client

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import sample.sharding.kafka.UserServiceClient
import sample.sharding.kafka.UserStatsRequest

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration
import scala.io.StdIn

object ClientApp extends App {
  implicit val system: ActorSystem = ActorSystem("UserClient")
  implicit val mat: Materializer = Materializer.createMaterializer(system)
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", 8081).withTls(false)
  val client = UserServiceClient(clientSettings)

  var userId = ""
  while (userId != ":q") {
    println("Enter user id or :q to quit")
    userId = StdIn.readLine()
    if (userId != ":q" && userId != null) {
      val runningTotal = Await.result(client.userStats(UserStatsRequest(userId)), Duration.Inf)
      println(
        s"User ${userId} has made ${runningTotal.totalPurchases} purchases for a total of ${runningTotal.amountSpent}p")
    }

  }
  println("Exiting")
  system.terminate()
}
