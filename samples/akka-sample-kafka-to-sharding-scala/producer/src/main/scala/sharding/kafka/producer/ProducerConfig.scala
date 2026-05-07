package sharding.kafka.producer

import com.typesafe.config.Config

case object ProducerConfig {
  def apply(config: Config): ProducerConfig =
    new ProducerConfig(
      config.getString("bootstrap-servers"),
      config.getString("topic"))
}

final class ProducerConfig(
    val bootstrapServers: String,
    val topic: String)
