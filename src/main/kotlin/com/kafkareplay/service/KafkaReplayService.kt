package com.kafkareplay.service

import com.kafkareplay.kafka.RetryTopicSender
import com.kafkareplay.mongo.dao.KafkaReplayDao
import com.kafkareplay.mongo.repository.KafkaReplayMongoRepository
import org.springframework.stereotype.Service
import java.util.*

@Service
class KafkaReplayService(
  private val kafkaReplayMongoRepository: KafkaReplayMongoRepository,
  private val retrySender: RetryTopicSender,
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

  fun getMessagesByTopic(topic: String) {
    kafkaReplayMongoRepository.findAllByTopic(topic)
  }

  fun getAllMessages() {
    kafkaReplayMongoRepository.findAll()
  }

  fun getAllTopics() {
    kafkaReplayMongoRepository.findAllTopicNames()
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

  fun retryAllMessagesByTopic(topic: String) {
    val messages = kafkaReplayMongoRepository.findAllByTopic(topic)
    for (message in messages) {
      retrySender.send(message.topic, message.key, message.payload)
      kafkaReplayMongoRepository.deleteById(message.id)
    }
  }
}