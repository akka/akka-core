package sample.sharding.embeddedkafka

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.slf4j.LoggerFactory

object KafkaBroker extends App with EmbeddedKafka {
  val log = LoggerFactory.getLogger(this.getClass)

  val port = 9092
  val topic = "user-events"
  val partitions = 128

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = port)
  val server = EmbeddedKafka.start()

  createCustomTopic(topic = topic, partitions = partitions)

  log.info(s"Kafka running: localhost:$port")
  log.info(s"Topic '$topic' with $partitions partitions created")

  server.broker.awaitShutdown()
}
