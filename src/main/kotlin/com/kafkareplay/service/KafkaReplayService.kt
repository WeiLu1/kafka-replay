package com.kafkareplay.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.kafkareplay.kafka.RetryTopicSender
import com.kafkareplay.mongo.repository.KafkaReplayMongoRepository
import org.slf4j.LoggerFactory.getLogger
import org.springframework.stereotype.Service
import java.util.*

@Service
class KafkaReplayService(
  private val kafkaReplayMongoRepository: KafkaReplayMongoRepository,
  private val retrySender: RetryTopicSender,
  private val objectMapper: ObjectMapper
) {

  companion object {
    private val logger = getLogger(KafkaReplayService::class.java)
  }

  fun deleteMessage(id: UUID) {
    kafkaReplayMongoRepository.deleteById(id)
  }

  fun deleteAllMessages() {
    kafkaReplayMongoRepository.deleteAll()
  }

  fun getMessage(id: UUID) {
    kafkaReplayMongoRepository.findById(id)
  }

  fun getAllMessages() {
    kafkaReplayMongoRepository.findAll()
  }

  fun retryMessage(id: UUID) {
    val message = objectMapper.writeValueAsString(kafkaReplayMongoRepository.findById(id))
    retrySender.send(message)
    kafkaReplayMongoRepository.deleteById(id)
  }

  fun retryAllMessages() {
    val everyDoc = kafkaReplayMongoRepository.findAll()
    for (doc in everyDoc) {
      runCatching {
        retrySender.send(doc.payload)
        kafkaReplayMongoRepository.deleteById(doc.id)
      }.onSuccess {
        logger.info("${doc.id} successfully sent and deleted from database.")
      }.onFailure { exception: Throwable ->
        logger.error("Something has gone wrong with ${doc.id}.")
        throw exception
      }
    }
  }
}