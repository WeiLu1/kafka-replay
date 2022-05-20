package com.kafkareplay.service

import com.kafkareplay.mongo.repository.KafkaReplayMongoRepository
import org.slf4j.LoggerFactory.getLogger
import org.springframework.stereotype.Service
import java.util.*

@Service
class KafkaReplayService(
  private val kafkaReplayMongoRepository: KafkaReplayMongoRepository
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
    val message = kafkaReplayMongoRepository.findById(id)
    TODO("send message back to retry topic")
    kafkaReplayMongoRepository.deleteById(id)
  }

  fun retryAllMessages() {
    TODO("send all messages back to the retry topic")
    kafkaReplayMongoRepository.deleteAll()
  }
}