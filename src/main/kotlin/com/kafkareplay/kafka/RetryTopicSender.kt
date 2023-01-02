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

  fun send(topic: String, key: String?, data: String, headers: Map<String, Any> = emptyMap()) {
    val messageBuilder = MessageBuilder
      .withPayload(data)
      .setHeader(KafkaHeaders.TOPIC, topic)
      .setHeader(KafkaHeaders.KEY, key)

    if (headers.isNotEmpty()) {
      headers.forEach {
        messageBuilder.setHeader(it.key, it.value)
      }
    }

   kafkaTemplate.send(messageBuilder.build())
  }

}
