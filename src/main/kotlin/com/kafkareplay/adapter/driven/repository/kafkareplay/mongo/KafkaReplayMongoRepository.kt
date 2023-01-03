package com.kafkareplay.adapter.driven.repository.kafkareplay.mongo

import org.springframework.data.mongodb.repository.MongoRepository
import java.util.*
import org.springframework.data.domain.Sort

interface KafkaReplayMongoRepository : MongoRepository<KafkaReplayEntity, UUID> {
  fun findAllByTopic(topic: String): List<KafkaReplayEntity>
  fun findAllByTopicAndKey(topic: String, key: String?, sort: Sort): List<KafkaReplayEntity>

  fun deleteAllByTopic(topic: String)
  fun deleteAllByTopicAndKey(topic: String, key: String?)

  fun findFirstByKeyAndTopicAndOriginalPositionReference_PartitionAndOriginalPositionReference_OffsetLessThan(key: String?, topic: String, partition: Int, offset: Long, sort: Sort): KafkaReplayEntity?

}