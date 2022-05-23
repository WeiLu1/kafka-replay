package com.kafkareplay.kafka

import com.kafkareplay.service.KafkaReplayService
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class ErrorTopicListener(
  private val kafkaReplayService: KafkaReplayService
) {

  companion object {
    private val LOG = LoggerFactory.getLogger(ErrorTopicListener::class.java)
  }

  @KafkaListener(topicPattern = ".*_ERROR", containerFactory = "kafkaListenerContainerFactory", groupId = "ms-kafka-replay")
  fun onErrorEvent(@Payload event: String,
                   @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
                   @Header(KafkaHeaders.DLT_EXCEPTION_STACKTRACE) exceptionMessage: String,
                   @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) key: String

  ) {
    LOG.info("Payload: {}", event)
    LOG.info("key: {}, event:{}", key, event)
    LOG.info("Error Message:{}", exceptionMessage)

    kafkaReplayService.saveMessage(topic, key, event, exceptionMessage)
  }
}