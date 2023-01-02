package com.kafkareplay.exception

import org.springframework.http.HttpStatus

abstract class KafkaReplayBaseException(
  open val error: KafkaReplayError,
  open val httpStatus: HttpStatus,
  errorMessage: String
) : RuntimeException(
  errorMessage
)
