package com.kafkareplay.config.application

import com.kafkareplay.adapter.driven.repository.kafkareplay.KafkaReplayRepository
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.KafkaReplayMongoRepository
import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class RepositoryConfig {
  @Bean
  fun kafkaReplayRepository(
    kafkaReplayMongoRepository: KafkaReplayMongoRepository
  ): IKafkaReplayRepository {
    return KafkaReplayRepository(
      kafkaReplayMongoRepository
    )
  }
}