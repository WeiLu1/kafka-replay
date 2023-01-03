package com.kafkareplay.adapter.driven.event.sender

import com.kafkareplay.application.port.adapter.driven.event.sender.IRetryTopicSender
import mu.KotlinLogging
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.support.MessageBuilder

class RetryTopicSender(
  private val kafkaTemplate: KafkaTemplate<String, Any>,
): IRetryTopicSender {

  companion object {
    private val LOG = KotlinLogging.logger {}
}

  override fun send(topic: String, key: String?, data: String, headers: Map<String, Any>) {
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
