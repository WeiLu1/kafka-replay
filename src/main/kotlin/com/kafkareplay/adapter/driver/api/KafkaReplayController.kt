package com.kafkareplay.adapter.driver.api

import com.kafkareplay.adapter.driver.api.response.KafkaReplayResponse
import com.kafkareplay.service.KafkaReplayService
import com.kafkareplay.utils.KafkaReplayConverter.convertToResponseDto
import org.springframework.web.bind.annotation.*
import java.util.*

@Deprecated("Please use V2")
@RestController
@RequestMapping("/internal/v1/messages")
class KafkaReplayController(
  private val kreplayService: KafkaReplayService
) {

  @DeleteMapping("/{id}")
  fun deleteMessage(
    @PathVariable("id") id: UUID
  ): KafkaReplayResponse {
    return convertToResponseDto(kreplayService.deleteMessage(id))
  }

  @DeleteMapping("/topic/{topic}")
  fun deleteAllMessagesByTopic(
    @PathVariable("topic") topic: String
  ): List<KafkaReplayResponse> {
    return kreplayService.deleteAllMessagesByTopic(topic).map {
      convertToResponseDto(it)
    }
  }

  @GetMapping("/{id}")
  fun getMessage(
    @PathVariable("id") id: UUID
  ): KafkaReplayResponse {
    return convertToResponseDto(kreplayService.getMessage(id))
  }

  @GetMapping("/topic/{topic}")
  fun getAllMessagesByTopic(
    @PathVariable("topic") topic: String
  ): List<KafkaReplayResponse> {
    return kreplayService.getMessagesByTopic(topic).map {
      convertToResponseDto(it)
    }
  }

  @GetMapping()
  fun getAllMessages(): List<KafkaReplayResponse> {
    return kreplayService.getAllMessages().map {
      convertToResponseDto(it)
    }
  }

  @GetMapping("/topics")
  fun getAllTopics(): List<String> {
    return kreplayService.getAllTopics()
  }

  @PostMapping("/{id}")
  fun retryMessage(
    @PathVariable("id") id: UUID
  ): KafkaReplayResponse {
    return convertToResponseDto(kreplayService.retryMessage(id))
  }

  @PostMapping("/topic/{topic}")
  fun retryAllMessagesByTopic(
    @PathVariable("topic") topic: String
  ): List<KafkaReplayResponse> {
    return kreplayService.retryAllMessagesByTopic(topic).map {
      convertToResponseDto(it)
    }
  }
}