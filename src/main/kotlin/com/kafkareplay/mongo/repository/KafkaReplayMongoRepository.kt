package com.kafkareplay.mongo.repository

import com.kafkareplay.mongo.dao.KafkaReplayDao
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import java.util.*
import org.springframework.data.domain.Sort

@Repository
interface KafkaReplayMongoRepository : MongoRepository<KafkaReplayDao, UUID> {
  fun findAllByTopic(topic: String): List<KafkaReplayDao>
  fun findAllByTopicAndKey(topic: String, key: String?, sort: Sort): List<KafkaReplayDao>

  fun deleteAllByTopic(topic: String)
  fun deleteAllByTopicAndKey(topic: String, key: String?)

  fun findFirstByKeyAndTopicAndOriginalPositionReference_PartitionAndOriginalPositionReference_OffsetLessThan(key: String?, topic: String, partition: Int, offset: Long, sort: Sort): KafkaReplayDao?

}