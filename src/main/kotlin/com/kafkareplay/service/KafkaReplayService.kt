package com.kafkareplay.service

import com.kafkareplay.exception.KafkaReplayNotFoundException
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


  fun deleteMessage(id: UUID): KafkaReplayDao {
    val message = getMessage(id)
    kafkaReplayMongoRepository.delete(message)
    return message
  }

  fun deleteAllMessagesByTopic(topic: String): List<KafkaReplayDao> {
    val messaages = getMessagesByTopic(topic)
    kafkaReplayMongoRepository.deleteAllByTopic(topic)
    return messaages
  }

  fun getMessage(id: UUID): KafkaReplayDao {
    return kafkaReplayMongoRepository.findById(id).orElseThrow {
      KafkaReplayNotFoundException(id)
    }
  }

  fun getMessagesByTopic(topic: String): List<KafkaReplayDao> {
    return kafkaReplayMongoRepository.findAllByTopic(topic)
  }

  fun getAllMessages(): List<KafkaReplayDao> {
    return kafkaReplayMongoRepository.findAll()
  }

  fun getAllTopics(): List<String> {
    return kafkaReplayMongoRepository.findAll().map {
      it.topic
    }.distinct()
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

  fun retryMessage(id: UUID): KafkaReplayDao {
    val message = getMessage(id)
    retrySender.send(message.topic, message.key, message.payload)
    return deleteMessage(id)
  }

  fun retryAllMessagesByTopic(topic: String): List<KafkaReplayDao> {
    val responseList = mutableListOf<KafkaReplayDao>()

    getMessagesByTopic(topic).forEach {
      retrySender.send(it.topic, it.key, it.payload)
      kafkaReplayMongoRepository.delete(it)
      responseList.add(it)
    }
    return responseList
  }

}