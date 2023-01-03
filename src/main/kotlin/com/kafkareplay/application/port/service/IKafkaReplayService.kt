package com.kafkareplay.application.port.service

import com.kafkareplay.domain.KafkaReplay
import java.util.*

interface IKafkaReplayService {
  fun findPreviousMessage(currentId: UUID): KafkaReplay?
  fun deleteByTopicAndKeys(topic: String, keys: Set<String?>)
  fun findAll(topic: String, keys: Set<String?>): Map<String?, List<KafkaReplay>>
}