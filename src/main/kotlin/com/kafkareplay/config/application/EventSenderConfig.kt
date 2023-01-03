package com.kafkareplay.config.application

import com.kafkareplay.adapter.driven.event.sender.RetryTopicSender
import com.kafkareplay.application.port.adapter.driven.event.sender.IRetryTopicSender
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate

@Configuration
class EventSenderConfig {
  @Bean
  fun retryTopicSender(
    kafkaTemplate: KafkaTemplate<String, Any>
  ): IRetryTopicSender {
    return RetryTopicSender(
      kafkaTemplate
    )
  }

}