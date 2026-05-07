package sharding.kafka.producer

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import sample.sharding.kafka.serialization.user_events.UserPurchaseProto

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

object UserEventProducer extends App {

  implicit val system: ActorSystem = ActorSystem(
    "UserEventProducer",
    ConfigFactory.parseString("""
      akka.actor.provider = "local" 
     """.stripMargin).withFallback(ConfigFactory.load()).resolve())

  val log = Logging(system, "UserEventProducer")

  val config = system.settings.config.getConfig("akka.kafka.producer")

  val producerConfig = ProducerConfig(system.settings.config.getConfig("kafka-to-sharding-producer"))

  val producerSettings: ProducerSettings[String, Array[Byte]] =
    ProducerSettings(config, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(producerConfig.bootstrapServers)

  val nrUsers = 200
  val maxPrice = 10000
  val maxQuantity = 5
  val products = List("cat t-shirt", "akka t-shirt", "skis", "climbing shoes", "rope")

  val done: Future[Done] =
    Source
      .tick(1.second, 1.second, "tick")
      .map(_ => {
        val randomEntityId = Random.nextInt(nrUsers).toString
        val price = Random.nextInt(maxPrice)
        val quantity = Random.nextInt(maxQuantity)
        val product = products(Random.nextInt(products.size))
        val message = UserPurchaseProto(randomEntityId, product, quantity, price).toByteArray
        log.info("Sending message to user {}", randomEntityId)
        // rely on the default kafka partitioner to hash the key and distribute among shards
        // the logic of the default partitioner must be replicated in MessageExtractor entityId -> shardId function
        new ProducerRecord[String, Array[Byte]](producerConfig.topic, randomEntityId, message)
      })
      .runWith(Producer.plainSink(producerSettings))
}
