package com.kafkareplay.mongo.repository

import com.kafkareplay.mongo.dao.KafkaReplayDao
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import java.util.*

@Repository
interface KafkaReplayMongoRepository : MongoRepository<KafkaReplayDao, UUID>