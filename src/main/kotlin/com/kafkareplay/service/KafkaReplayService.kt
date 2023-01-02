package com.kafkareplay.service

import com.kafkareplay.exception.KafkaReplayNotFoundException
import com.kafkareplay.kafka.RetryTopicSender
import com.kafkareplay.mongo.dao.KafkaReplayDao
import com.kafkareplay.mongo.dao.KafkaTopicOrder
import com.kafkareplay.mongo.dao.PositionReferenceId
import com.kafkareplay.mongo.repository.KafkaReplayMongoRepository
import com.kafkareplay.utils.KafkaReplayConverter
import org.springframework.stereotype.Service
import java.util.*
import mu.KotlinLogging

@Service
class KafkaReplayService(
  private val kafkaReplayMongoRepository: KafkaReplayMongoRepository,
  private val retrySender: RetryTopicSender
) {

  companion object {
    private val LOG = KotlinLogging.logger {}
  }

  fun deleteMessage(id: UUID): KafkaReplayDao {
    val message = getMessage(id)
    kafkaReplayMongoRepository.delete(message)
    return message
  }

  fun deleteAllMessagesByTopic(topic: String): List<KafkaReplayDao> {
    val messages = getMessagesByTopic(topic)
    kafkaReplayMongoRepository.deleteAllByTopic(topic)
    return messages
  }

  fun getMessage(id: UUID): KafkaReplayDao {
    return kafkaReplayMongoRepository.findById(id).orElseThrow {
      KafkaReplayNotFoundException(id)
    }
  }

  fun getMessagesByTopic(topic: String): List<KafkaReplayDao> {
    return kafkaReplayMongoRepository.findAllByTopic(
      topic
    )
  }

  fun getAllMessages(): List<KafkaReplayDao> {
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

    val kafkaReplay = KafkaReplayDao(
      id = uuid,
      topic = topic.replace("_ERROR", "_RETRY"),
      key = key,
      payload = payload,
      exceptionStacktrace = exceptionStacktrace,
      headers = headers,
      originalPositionReference = PositionReferenceId(originalPartition, originalOffset, topic.replace("_ERROR", "_RETRY"), KafkaTopicOrder.SORTED)
    )
    kafkaReplayMongoRepository.save(kafkaReplay)
  }

  fun retryMessage(id: UUID): KafkaReplayDao {
    val message = getMessage(id)
    retrySender.send(message.topic, message.key, KafkaReplayConverter.decodeBase64(message.payload))
    return deleteMessage(id)
  }

  fun retryAllMessagesByTopic(topic: String): List<KafkaReplayDao> {
    val responseList = getMessagesByTopic(topic)

    responseList.forEach {
      retrySender.send(it.topic, it.key, KafkaReplayConverter.decodeBase64(it.payload))
      kafkaReplayMongoRepository.delete(it)
    }

    return responseList
  }

}