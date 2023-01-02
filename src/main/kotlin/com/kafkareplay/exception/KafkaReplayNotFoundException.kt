package com.kafkareplay.exception

import java.util.*
import org.springframework.http.HttpStatus


class KafkaReplayNotFoundException(
  id: UUID
): KafkaReplayBaseException(
  KafkaReplayError.KAFKA_REPLAY_N0T_FOUND,
  HttpStatus.NOT_FOUND,
  KafkaReplayError.KAFKA_REPLAY_N0T_FOUND.errorMessage.format(id)
)