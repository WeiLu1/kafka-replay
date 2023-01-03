package com.kafkareplay.application.port.adapter.driven.repository.kafkareplay

import com.kafkareplay.domain.KafkaReplay
import java.util.*

interface IKafkaReplayRepository {
  fun save(kafkaReplay: KafkaReplay): KafkaReplay
  fun getAll(): List<KafkaReplay>
  fun getById(id: UUID): KafkaReplay?
  fun getAllByTopic(topic: String): List<KafkaReplay>
  fun getAllByTopicAndKeys(topic: String, key: String?): List<KafkaReplay>
  fun getPreviousReplayMessage(key: String?, topic: String, partition: Int, offset: Long): KafkaReplay?
  fun deleteById(id: UUID)
  fun deleteByTopicAndKey(topic: String, key: String?)
}