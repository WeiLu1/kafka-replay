package com.kafkareplay.commons.exception

import java.util.*
import org.springframework.http.HttpStatus


class UnableToDeleteKafkaMessageException(
  previousId: UUID,
  currentId: UUID,
): KafkaReplayBaseException(
  KafkaReplayError.UNABLE_TO_DELETE_CURRENT_MESSAGE,
  HttpStatus.BAD_REQUEST,
  KafkaReplayError.UNABLE_TO_DELETE_CURRENT_MESSAGE.errorMessage.format(currentId, previousId)
)