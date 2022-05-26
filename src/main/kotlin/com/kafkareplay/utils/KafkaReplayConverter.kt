package com.kafkareplay.utils

import com.kafkareplay.model.dto.KafkaReplayResponse
import com.kafkareplay.mongo.dao.KafkaReplayDao

object KafkaReplayConverter {

  fun convertToResponseDto(kafkaReplayDao: KafkaReplayDao): KafkaReplayResponse {
    return KafkaReplayResponse(
      id =  kafkaReplayDao.id,
      topic = kafkaReplayDao.topic,
      key = kafkaReplayDao.key,
      payload = kafkaReplayDao.payload,
      exceptionStacktrace = kafkaReplayDao.exceptionStacktrace
    )
  }
}