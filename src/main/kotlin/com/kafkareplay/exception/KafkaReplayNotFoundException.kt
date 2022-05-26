package com.kafkareplay.exception

import java.util.*


class KafkaReplayNotFoundException(
  private val id: UUID
): RuntimeException()