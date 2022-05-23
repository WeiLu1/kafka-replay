package com.kafkareplay.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.kafkareplay.kafka.RetryTopicSender
import com.kafkareplay.mongo.dao.KafkaReplayDao
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

  fun saveMessage(topic: String, key: String, payload: String, exceptionStacktrace: String) {
    val uuid = UUID.randomUUID()
    val obj = KafkaReplayDao(
      id = uuid,
      topic = topic.replace("_ERROR", "_RETRY"),
      key = key,
      payload = payload,
      exceptionStacktrace = exceptionStacktrace
    )
    kafkaReplayMongoRepository.save(obj)
  }

  fun retryMessage(id: UUID) {
    val message = kafkaReplayMongoRepository.findById(id)
    retrySender.send(message.get().topic, message.get().key, message.get().payload)
    kafkaReplayMongoRepository.deleteById(id)
  }

  fun retryAllMessages(topic: String) {
    TODO("Front end will have drop down to select all messages in topic to retry")
//    val everyDoc = kafkaReplayMongoRepository.findAll()
//    for (doc in everyDoc) {
//      retrySender.send(doc.get().topic, doc.get().key, doc.get().payload)
//      kafkaReplayMongoRepository.deleteById(doc.id)
//    }
  }
}