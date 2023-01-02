package com.kafkareplay.exception

enum class KafkaReplayError(val errorCode: String, val errorMessage: String) {
  INTERNAL_ERROR(
    "MS_KAFA_REPLAY-000",
    "Internal Error has occured due to: %s"
  ),
  VALIDATION_ERROR(
    "MS_KAFA_REPLAY-001",
    "%s"
  ),
  MALFORMED_JSON_ERROR(
    "MS_KAFA_REPLAY-002",
    "Malformed JSON request JSON parse error due to: %s"
  ),
  KAFKA_REPLAY_N0T_FOUND(
    "MS_KAFA_REPLAY-003",
    "Kafka Replay not found for ID:[%s]"
  ),
  UNABLE_TO_REPLAY_CURRENT_MESSAGE(
    "MS_KAFA_REPLAY-004",
    "Not able to Retry Current Message for ID:[%s]. Please try to Replay previous message with ID:[%s] in order to Replay the Current Message"
  ),
  ORIGINAL_POSITION_REFERENCE_NOT_FOUND(
    "MS_KAFA_REPLAY-005",
    "Position Reference not found for Message ID:[%s]. Please Investigate and fix this"
  ),
  UNABLE_TO_DELETE_CURRENT_MESSAGE(
    "MS_KAFA_REPLAY-004",
    "Not able to Delete this Current Message for ID:[%s]. Please try to Delete previous message with ID:[%s] in order to Delete the Current Message"
  )
}
