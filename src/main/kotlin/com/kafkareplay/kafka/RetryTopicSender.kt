package com.kafkareplay.kafka

import mu.KotlinLogging
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Component

@Component
class RetryTopicSender(
  private val kafkaTemplate: KafkaTemplate<String, Any>,
) {

  companion object {
    private val LOG = KotlinLogging.logger {}
}

  fun send(topic: String, key: String, data: String) {
    val message = MessageBuilder
      .withPayload(data)
      .setHeader(KafkaHeaders.TOPIC, topic)
      .setHeader(KafkaHeaders.KEY, key)
      .build();

   kafkaTemplate.send(message)
  }
}
