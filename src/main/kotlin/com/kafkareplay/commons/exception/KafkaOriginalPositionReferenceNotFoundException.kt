package com.kafkareplay.commons.exception

import java.util.*
import org.springframework.http.HttpStatus


class KafkaOriginalPositionReferenceNotFoundException(
  id: UUID,
): KafkaReplayBaseException(
  KafkaReplayError.ORIGINAL_POSITION_REFERENCE_NOT_FOUND,
  HttpStatus.NOT_FOUND,
  KafkaReplayError.ORIGINAL_POSITION_REFERENCE_NOT_FOUND.errorMessage.format(id)
)