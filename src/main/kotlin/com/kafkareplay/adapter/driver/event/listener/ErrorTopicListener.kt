package com.kafkareplay.adapter.driver.event.listener

import com.kafkareplay.application.port.usecase.ISaveKafkaErrorEventUseCase
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class ErrorTopicListener(
  private val saveKafkaErrorEventUseCase: ISaveKafkaErrorEventUseCase
) {

  companion object {
    private val LOG = KotlinLogging.logger {}
  }

  @KafkaListener(topicPattern = ".*_ERROR", containerFactory = "kafkaListenerContainerFactory", groupId = "ms-kafka-replay")
  fun onErrorEvent(@Payload event: ConsumerRecord<String, String>,
                   @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
                   @Header(KafkaHeaders.DLT_EXCEPTION_STACKTRACE) exceptionMessage: String,
                   @Header(KafkaHeaders.RECEIVED_KEY) key: String?
  ) {
    LOG.info("Consuming Message from topic:[$topic] with key:[$key], partition:[${event.partition()}], offset:[${event.offset()}].")
    val headers = event.headers().associate { Pair(it.key(), it.value()) }
    saveKafkaErrorEventUseCase.invoke(
      topic = topic,
      key = key,
      payload = event.value(),
      exceptionStacktrace = exceptionMessage,
      headers = headers
    )
  }
}