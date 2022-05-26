package com.kafkareplay.controller

import com.kafkareplay.model.dto.KafkaReplayResponse
import com.kafkareplay.service.KafkaReplayService
import com.kafkareplay.utils.KafkaReplayConverter.convertToResponseDto
import org.springframework.web.bind.annotation.*
import java.util.*

@RestController
@RequestMapping("/internal/v1/messages")
class KafkaReplayController(
  private val kafkaReplayService: KafkaReplayService
) {

  @DeleteMapping("/{id}")
  fun deleteMessage(
    @PathVariable("id") id: UUID
  ): KafkaReplayResponse {
    return convertToResponseDto(kafkaReplayService.deleteMessage(id))
  }

  @DeleteMapping("/topic/{topic}")
  fun deleteAllMessagesByTopic(
    @PathVariable("topic") topic: String
  ): List<KafkaReplayResponse> {
    return kafkaReplayService.deleteAllMessagesByTopic(topic).map {
      convertToResponseDto(it)
    }
  }

  @GetMapping("/{id}")
  fun getMessage(
    @PathVariable("id") id: UUID
  ): KafkaReplayResponse {
    return convertToResponseDto(kafkaReplayService.getMessage(id))
  }

  @GetMapping("/topic/{topic}")
  fun getAllMessagesByTopic(
    @PathVariable("topic") topic: String
  ): List<KafkaReplayResponse> {
    return kafkaReplayService.getMessagesByTopic(topic).map {
      convertToResponseDto(it)
    }
  }

  @GetMapping()
  fun getAllMessages(): List<KafkaReplayResponse> {
    return kafkaReplayService.getAllMessages().map {
      convertToResponseDto(it)
    }
  }

  @GetMapping("/topics")
  fun getAllTopics(): List<String> {
    return kafkaReplayService.getAllTopics()
  }

  @PostMapping("/{id}")
  fun retryMessage(
    @PathVariable("id") id: UUID
  ): KafkaReplayResponse {
    return convertToResponseDto(kafkaReplayService.retryMessage(id))
  }

  @PostMapping("/topic/{topic}")
  fun retryAllMessagesByTopic(
    @PathVariable("topic") topic: String
  ) {
    kafkaReplayService.retryAllMessagesByTopic(topic)
  }
}