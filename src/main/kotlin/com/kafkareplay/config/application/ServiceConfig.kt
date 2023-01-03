package com.kafkareplay.config.application

import com.kafkareplay.adapter.driven.repository.kafkareplay.KafkaReplayRepository
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.KafkaReplayMongoRepository
import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.service.IKafkaReplayService
import com.kafkareplay.application.service.KafkaReplayService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ServiceConfig {
  @Bean
  fun kafkaReplayService(
    kafkaReplayRepository: IKafkaReplayRepository,
  ): IKafkaReplayService {
    return KafkaReplayService(
      kafkaReplayRepository
    )
  }
}