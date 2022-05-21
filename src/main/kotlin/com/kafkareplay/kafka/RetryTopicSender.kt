package com.kafkareplay.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.kafkareplay.model.dto.KafkaReplayDto
import com.kafkareplay.service.KafkaReplayService
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Component

@Component
class RetryTopicSender(
  private val kafkaTemplate: KafkaTemplate<String, String>,
  private val objectMapper: ObjectMapper
) {

  companion object {
    private val logger = LoggerFactory.getLogger(KafkaReplayService::class.java)
}

  fun send(
    payload: String,
  ) {
    val topic = objectMapper.readValue(payload, KafkaReplayDto::class.java).topic
    kafkaTemplate.send(
      MessageBuilder
        .withPayload(payload)
        .setHeader(KafkaHeaders.TOPIC, topic)
        .build()
    )
    logger.info("Message sent")
  }
}
