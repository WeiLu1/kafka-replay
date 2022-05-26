package com.kafkareplay.mongo.repository

import com.kafkareplay.mongo.dao.KafkaReplayDao
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.data.mongodb.repository.Query
import org.springframework.stereotype.Repository
import java.util.*

@Repository
interface KafkaReplayMongoRepository : MongoRepository<KafkaReplayDao, UUID> {
  fun findAllByTopic(topic: String): List<KafkaReplayDao>

  fun deleteAllByTopic(topic: String)
}