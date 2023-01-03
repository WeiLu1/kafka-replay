package com.kafkareplay.application.port.usecase

interface IGetAllKafkaReplayTopicUseCase {
  fun invoke(): Set<String>
}