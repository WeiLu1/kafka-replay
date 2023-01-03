package com.kafkareplay.service

import com.kafkareplay.commons.exception.KafkaReplayNotFoundException
import com.kafkareplay.adapter.driven.event.sender.RetryTopicSender
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.KafkaReplayEntity
import com.kafkareplay.domain.KafkaTopicOrder
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.PositionReferenceEntity
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.KafkaReplayMongoRepository
import com.kafkareplay.application.port.adapter.driven.event.sender.IRetryTopicSender
import com.kafkareplay.utils.KafkaReplayConverter
import org.springframework.stereotype.Service
import java.util.*
import mu.KotlinLogging

@Deprecated("Please use V2")
@Service("kreplayService")
class KafkaReplayService(
  private val kafkaReplayMongoRepository: KafkaReplayMongoRepository,
  private val retrySender: IRetryTopicSender
) {

  companion object {
    private val LOG = KotlinLogging.logger {}
  }

  fun deleteMessage(id: UUID): KafkaReplayEntity {
    val message = getMessage(id)
    kafkaReplayMongoRepository.delete(message)
    return message
  }

  fun deleteAllMessagesByTopic(topic: String): List<KafkaReplayEntity> {
    val messages = getMessagesByTopic(topic)
    kafkaReplayMongoRepository.deleteAllByTopic(topic)
    return messages
  }

  fun getMessage(id: UUID): KafkaReplayEntity {
    return kafkaReplayMongoRepository.findById(id).orElseThrow {
      KafkaReplayNotFoundException(id)
    }
  }

  fun getMessagesByTopic(topic: String): List<KafkaReplayEntity> {
    return kafkaReplayMongoRepository.findAllByTopic(
      topic
    )
  }

  fun getAllMessages(): List<KafkaReplayEntity> {
    return kafkaReplayMongoRepository.findAll()
  }

  fun getAllTopics(): List<String> {
    return kafkaReplayMongoRepository.findAll().map {
      it.topic
    }.distinct()
  }

  fun saveMessage(topic: String, key: String?, payload: String, exceptionStacktrace: String, headers: Map<String, ByteArray>) {
    val uuid = UUID.randomUUID()

    val referenceId = headers["RETRYING_REFERENCE_ID"]?.let { String(it).split(":") }
    LOG.info("Header found: $referenceId")
    val originalPartition = referenceId?.get(0)?.toInt()
    val originalOffset = referenceId?.get(1)?.toLong()

    val kafkaReplay = KafkaReplayEntity(
      id = uuid,
      topic = topic.replace("_ERROR", "_RETRY"),
      key = key,
      payload = payload,
      exceptionStacktrace = exceptionStacktrace,
      headers = headers,
      originalPositionReference = PositionReferenceEntity(originalPartition, originalOffset, topic.replace("_ERROR", "_RETRY"), KafkaTopicOrder.SORTED)
    )
    kafkaReplayMongoRepository.save(kafkaReplay)
  }

  fun retryMessage(id: UUID): KafkaReplayEntity {
    val message = getMessage(id)
    retrySender.send(message.topic, message.key, KafkaReplayConverter.decodeBase64(message.payload))
    return deleteMessage(id)
  }

  fun retryAllMessagesByTopic(topic: String): List<KafkaReplayEntity> {
    val responseList = getMessagesByTopic(topic)

    responseList.forEach {
      retrySender.send(it.topic, it.key, KafkaReplayConverter.decodeBase64(it.payload))
      kafkaReplayMongoRepository.delete(it)
    }

    return responseList
  }

}