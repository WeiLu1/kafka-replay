package com.kafkareplay.kafka

import com.kafkareplay.mongo.dao.KafkaTopicOrder
import com.kafkareplay.service.KafkaReplayService
import com.kafkareplay.service.KafkaReplayServiceV2
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
  private val kafkaReplayService: KafkaReplayService,
  private val kafkaReplayServiceV2: KafkaReplayServiceV2,
) {

  companion object {
    private val LOG = KotlinLogging.logger {}
  }

  @KafkaListener(topicPattern = ".*_ERROR", containerFactory = "kafkaListenerContainerFactory", groupId = "ms-kafka-replay")
  fun onErrorEvent(@Payload event: ConsumerRecord<String, String>,
                   @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
                   @Header(KafkaHeaders.DLT_EXCEPTION_STACKTRACE) exceptionMessage: String,
                   @Header(KafkaHeaders.RECEIVED_KEY) key: String?,
                   @Header("RETRYING_ORDER") retryingOrder: String?

  ) {
    LOG.info("Consuming Message from topic:[$topic] with key:[$key], partition:[${event.partition()}], offset:[${event.offset()}].")

    val headers = event.headers().associate { Pair(it.key(), it.value()) }

    val order = retryingOrder.let { if (it == "SORTED") KafkaTopicOrder.SORTED else KafkaTopicOrder.UNSORTED  }

    kafkaReplayServiceV2.saveMessage(topic, key, convertToBase64(event.value()), exceptionMessage, headers, order)
  }
}