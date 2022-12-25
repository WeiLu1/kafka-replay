package com.kafkareplay.kafka

import com.kafkareplay.service.KafkaReplayService
import com.kafkareplay.utils.KafkaReplayConverter.convertToBase64
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
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
    private val LOG = KotlinLogging.logger {}
  }

  @KafkaListener(topicPattern = ".*_ERROR", containerFactory = "kafkaListenerContainerFactory", groupId = "ms-kafka-replay")
  fun onErrorEvent(@Payload event: ConsumerRecord<String, String>,
                   @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
                   @Header(KafkaHeaders.DLT_EXCEPTION_STACKTRACE) exceptionMessage: String,
                   @Header(KafkaHeaders.RECEIVED_KEY) key: String

  ) {
    LOG.info("Payload: {} - Base64: {}", event.value(), convertToBase64(event.value()))
    LOG.info("key: {}, event:{}", event.key(), event)
    LOG.info("Error Message:{}", exceptionMessage)

    val headers = event.headers().associate { Pair(it.key(), it.value()) }
    LOG.info("Headers:{}", headers)

    kafkaReplayService.saveMessage(topic, key, convertToBase64(event.value()), exceptionMessage, headers)
  }
}