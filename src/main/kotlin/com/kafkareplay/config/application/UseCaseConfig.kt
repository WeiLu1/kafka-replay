package com.kafkareplay.config.application

import com.kafkareplay.application.port.adapter.driven.event.sender.IRetryTopicSender
import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.service.IKafkaReplayService
import com.kafkareplay.application.port.usecase.*
import com.kafkareplay.application.usecase.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class UseCaseConfig {
  @Bean
  fun deleteKafkaMessageUseCase(
    kafkaReplayRepository: IKafkaReplayRepository,
    kafkaReplayService: IKafkaReplayService
  ): IDeleteKafkaMessageUseCase {
    return DeleteKafkaMessageUseCase(
      kafkaReplayService,
      kafkaReplayRepository
    )
  }

  @Bean
  fun replayKafkaMessageUseCase(
    kafkaReplayRepository: IKafkaReplayRepository,
    kafkaReplayService: IKafkaReplayService,
    retryTopicSender: IRetryTopicSender
  ): IReplayKafkaMessageUseCase {
    return ReplayKafkaMessageUseCase(
      kafkaReplayService,
      kafkaReplayRepository,
      retryTopicSender
    )
  }

  @Bean
  fun saveKafkaErrorEventUseCase(
    kafkaReplayRepository: IKafkaReplayRepository
  ): ISaveKafkaErrorEventUseCase {
    return SaveKafkaErrorEventUseCase(
      kafkaReplayRepository
    )
  }

  @Bean
  fun getAllReplayKafkaKeyUseCase(
    kafkaReplayRepository: IKafkaReplayRepository
  ): IGetAllKafkaReplayKeyUseCase {
    return GetAllReplayKafkaKeyUseCase(
      kafkaReplayRepository
    )
  }

  @Bean
  fun getAllReplayKafkaTopicUseCase(
    kafkaReplayRepository: IKafkaReplayRepository
  ): IGetAllKafkaReplayTopicUseCase {
    return GetAllReplayKafkaTopicUseCase(
      kafkaReplayRepository
    )
  }

  @Bean
  fun getAllReplayKafkaMessagesUseCase(
    kafkaReplayRepository: IKafkaReplayRepository
  ): IGetAllKafkaReplayMessagesUseCase {
    return GetAllReplayKafkaMessagesUseCase(
      kafkaReplayRepository
    )
  }

  @Bean
  fun getReplayKafkaMessageUseCase(
    kafkaReplayRepository: IKafkaReplayRepository
  ): IGetKafkaReplayMessageUseCase {
    return GetReplayKafkaMessageUseCase(
      kafkaReplayRepository
    )
  }
}