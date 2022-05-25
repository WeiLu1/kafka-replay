package com.kafkareplay.controller

import com.kafkareplay.service.KafkaReplayService
import org.springframework.web.bind.annotation.*
import java.util.*

@RestController
@RequestMapping("/internal/v1")
class KafkaReplayController(
  private val kafkaReplayService: KafkaReplayService
) {

  @DeleteMapping("/{id}")
  fun deleteMessage(
    @PathVariable("id") id: UUID
  ) {
    kafkaReplayService.deleteMessage(id)
  }

  @DeleteMapping
  fun deleteAllMessages() {
    kafkaReplayService.deleteAllMessages()
  }

  @GetMapping("/{id}")
  fun getMessage(
    @PathVariable("id") id: UUID
  ) {
    kafkaReplayService.getMessage(id)
  }

  @GetMapping("/{topic}")
  fun getAllMessagesByTopic(
    @PathVariable("topic") topic: String
  ) {
    kafkaReplayService.getMessagesByTopic(topic)
  }

  @GetMapping()
  fun getAllMessages() {
    kafkaReplayService.getAllMessages()
  }

  @PostMapping("/{id}")
  fun retryMessage(
    @PathVariable("id") id: UUID
  ) {
    kafkaReplayService.retryMessage(id)
  }

  @PostMapping("/{topic}")
  fun retryAllMessages(
    @PathVariable("topic") topic: String
  ) {
    kafkaReplayService.retryAllMessagesByTopic(topic)
  }

  @GetMapping("/topics")
  fun getAllTopics() {
    kafkaReplayService.getAllTopics()
  }
}