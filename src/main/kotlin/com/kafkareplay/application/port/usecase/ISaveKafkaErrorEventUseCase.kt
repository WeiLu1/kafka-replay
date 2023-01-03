package com.kafkareplay.application.port.usecase

interface ISaveKafkaErrorEventUseCase {
  fun invoke(topic: String, key: String?, payload: String, exceptionStacktrace: String, headers: Map<String, ByteArray>)
}