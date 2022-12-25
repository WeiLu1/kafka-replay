package com.kafkareplay.kafka

import com.kafkareplay.service.KafkaReplayService
import org.apache.kafka.common.utils.Bytes
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Component
import kotlin.reflect.typeOf

@Component
class RetryTopicSender(
  private val kafkaTemplate: KafkaTemplate<String, Any>,
) {

  companion object {
    private val logger = LoggerFactory.getLogger(KafkaReplayService::class.java)
}

  fun send(topic: String, key: String, data: String) {
    val message = MessageBuilder
      .withPayload(data)
      .setHeader(KafkaHeaders.TOPIC, topic)
      .setHeader(KafkaHeaders.MESSAGE_KEY, key)
      .build();

   kafkaTemplate.send(message)
  }
}
