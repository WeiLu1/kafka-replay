package com.kafkareplay.service

import com.kafkareplay.exception.KafkaOriginalPositionReferenceNotFoundException
import com.kafkareplay.exception.KafkaReplayNotFoundException
import com.kafkareplay.exception.UnableToDeleteKafkaMessageException
import com.kafkareplay.exception.UnableToReplayKafkaMessageException
import com.kafkareplay.kafka.RetryTopicSender
import com.kafkareplay.mongo.dao.KafkaReplayDao
import com.kafkareplay.mongo.dao.KafkaTopicOrder
import com.kafkareplay.mongo.dao.PositionReferenceId
import com.kafkareplay.mongo.repository.KafkaReplayMongoRepository
import com.kafkareplay.utils.KafkaReplayConverter
import com.kafkareplay.utils.KafkaReplayV2Converter.decodeBase64
import org.springframework.stereotype.Service
import java.util.*
import mu.KotlinLogging
import org.springframework.data.domain.Sort

@Service
class KafkaReplayServiceV2(
  private val kafkaReplayMongoRepository: KafkaReplayMongoRepository,
  private val retrySender: RetryTopicSender
) {

  companion object {
    private val LOG = KotlinLogging.logger {}
  }

  fun saveMessage(topic: String, key: String?, payload: String, exceptionStacktrace: String, headers: Map<String, ByteArray>, order: KafkaTopicOrder) {
    try {
      val uuid = UUID.randomUUID()

      val referenceId = headers["RETRYING_REFERENCE_ID"]?.let { String(it).split(":") }
      LOG.info("Header found: $referenceId")
      val originalPartition = referenceId?.get(0)?.toInt()
      val originalOffset = referenceId?.get(1)?.toLong()
      val targetTopic = topic.replace("_ERROR", "_RETRY")
      val positionReference = PositionReferenceId(originalPartition, originalOffset, targetTopic, order)

      val kafkaReplay = KafkaReplayDao(
        id = uuid,
        topic = targetTopic,
        key = key,
        payload = payload,
        exceptionStacktrace = exceptionStacktrace,
        headers = headers,
        originalPositionReference = positionReference
      )
      kafkaReplayMongoRepository.save(kafkaReplay)
    } catch (e: Exception) {
      LOG.warn("Failed to save message for topic:[$topic] and key:[$key]")
      throw e
    }

  }

  fun getAllTopics(): List<String> {
    return kafkaReplayMongoRepository.findAll().map {
      it.topic
    }.distinct()
  }

  fun getAllKeys(topic: String): List<String?> {
    return kafkaReplayMongoRepository.findAllByTopic(topic = topic).map {
      it.key
    }.distinct()
  }

  fun getMessages(topic: String, keys: Set<String?>): Map<String?, List<KafkaReplayDao>> {
    return keys.associateWith { key -> getMessages(topic, key) }
  }

  fun deleteMessages(topic: String, keys: Set<String?>) {
    keys.associateWith { key -> kafkaReplayMongoRepository.deleteAllByTopicAndKey(topic, key) }
  }

  fun deleteMessage(id: UUID): KafkaReplayDao {
    val currentMessage = getMessage(id)

    if (currentMessage.originalPositionReference.order == KafkaTopicOrder.SORTED) {
      val previousMessage = findPreviousMessage(currentMessage)
      if (previousMessage != null) {
        throw UnableToDeleteKafkaMessageException(previousMessage.id, currentMessage.id)
      }
    }

    kafkaReplayMongoRepository.delete(currentMessage)
    return currentMessage
  }

  fun retryMessages(topic: String, keys: Set<String?>) {
    val messages = getMessages(topic, keys)
    validatedMessages(messages)

    messages.keys.forEach { key ->
      messages[key]?.forEach { message ->
        retrySender.send(topic, key, decodeBase64(message.payload), message.headers)
      }

      kafkaReplayMongoRepository.deleteAllByTopicAndKey(topic, key)
    }
  }

  fun retryMessage(id: UUID) {
    val currentMessage = getMessage(id)
    if (currentMessage.originalPositionReference.order == KafkaTopicOrder.SORTED) {
      val previousMessage = findPreviousMessage(currentMessage)
      if (previousMessage != null) {
        throw UnableToReplayKafkaMessageException(previousMessage.id, currentMessage.id)
      }
    }

    retrySender.send(currentMessage.topic, currentMessage.key, KafkaReplayConverter.decodeBase64(currentMessage.payload), currentMessage.headers)
    kafkaReplayMongoRepository.delete(currentMessage)
  }

  fun getMessage(id: UUID): KafkaReplayDao {
    val p = kafkaReplayMongoRepository.findById(id).orElseThrow {
      KafkaReplayNotFoundException(id)
    }

    p.headers.forEach {
      LOG.info("Headers[${it.key}] = ${String(it.value)}")
    }

    return p

  }

  private fun getMessages(topic: String, key: String?): List<KafkaReplayDao> {
    return kafkaReplayMongoRepository.findAllByTopicAndKey(
      topic,
      key,
      Sort.by(
        Sort.Order(Sort.Direction.ASC, "originalPositionReference.partition"),
        Sort.Order(Sort.Direction.ASC, "originalPositionReference.offset")
      )
    )
  }

  private fun findPreviousMessage(currentMessage: KafkaReplayDao): KafkaReplayDao? {
    if (currentMessage.originalPositionReference.partition == null || currentMessage.originalPositionReference.offset == null) {
      throw KafkaOriginalPositionReferenceNotFoundException(currentMessage.id)
    }

    val previousMessage = kafkaReplayMongoRepository.findFirstByKeyAndTopicAndOriginalPositionReference_PartitionAndOriginalPositionReference_OffsetLessThan(
      currentMessage.key,
      currentMessage.topic,
      currentMessage.originalPositionReference.partition,
      currentMessage.originalPositionReference.offset,
      Sort.by(
        Sort.Order(Sort.Direction.ASC, "originalPositionReference.partition"),
        Sort.Order(Sort.Direction.DESC, "originalPositionReference.offset")
      )
    )

    return previousMessage
  }

  private fun validatedMessages(messages: Map<String?, List<KafkaReplayDao>>) {
    messages.keys.forEach {
      messages[it]?.forEach { message ->
        if (message.originalPositionReference.order == KafkaTopicOrder.SORTED && (message.originalPositionReference.partition == null || message.originalPositionReference.offset == null)) {
          throw KafkaOriginalPositionReferenceNotFoundException(message.id)
        }
      }
    }
  }
}