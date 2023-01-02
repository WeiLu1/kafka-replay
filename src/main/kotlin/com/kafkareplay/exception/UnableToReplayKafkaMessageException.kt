package com.kafkareplay.exception

import java.util.*
import org.springframework.http.HttpStatus


class UnableToReplayKafkaMessageException(
  previousId: UUID,
  currentId: UUID,
): KafkaReplayBaseException(
  KafkaReplayError.UNABLE_TO_REPLAY_CURRENT_MESSAGE,
  HttpStatus.BAD_REQUEST,
  KafkaReplayError.UNABLE_TO_REPLAY_CURRENT_MESSAGE.errorMessage.format(currentId, previousId)
)