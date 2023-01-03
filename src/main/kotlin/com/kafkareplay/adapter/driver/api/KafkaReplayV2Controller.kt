package com.kafkareplay.adapter.driver.api

import com.kafkareplay.adapter.driver.api.mapper.KafkaReplayV2Converter
import com.kafkareplay.commons.exception.ErrorResponse
import com.kafkareplay.adapter.driver.api.response.KafkaReplayMessage
import com.kafkareplay.adapter.driver.api.response.KafkaReplayWrapper
import com.kafkareplay.adapter.driver.api.request.ReplayRequest
import com.kafkareplay.application.port.usecase.*
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.ArraySchema
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import org.springframework.web.bind.annotation.*
import java.util.*
import mu.KotlinLogging
import org.springframework.http.HttpStatus
import org.springframework.validation.annotation.Validated

@CrossOrigin("*")
@RestController
@Validated
@RequestMapping("/internal/v2/messages")
@Tag(name = "Kafka Replay", description = "Kafka Replay Endpoints for Retrying Failed Messages")
class KafkaReplayV2Controller(
  private val getReplayKafkaMessageUseCase: IGetKafkaReplayMessageUseCase,
  private val deleteKafkaMessageUseCase: IDeleteKafkaMessageUseCase,
  private val getAllKafkaReplayMessagesUseCase: IGetAllKafkaReplayMessagesUseCase,
  private val getAllKafkaReplayTopicUseCase: IGetAllKafkaReplayTopicUseCase,
  private val getAllKafkaReplayKeyUseCase: IGetAllKafkaReplayKeyUseCase,
  private val replayKafkaMessageUseCase: IReplayKafkaMessageUseCase
) {
  companion object {
    private val LOG = KotlinLogging.logger {}
  }

  @ResponseStatus(HttpStatus.NO_CONTENT)
  @Operation(
    summary = "Delete Message",
    description = "Delete Message by Replay ID",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "204",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = Unit::class, hidden = true))
        ]
      ),
      ApiResponse(
        description = "Not found",
        responseCode = "404",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "500",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Bad Request",
        responseCode = "400",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      )
    ]
  )
  @DeleteMapping("/{id}")
  fun deleteMessage(
    @PathVariable("id") @Parameter(description = "Replay ID", example = "10ab1276-663d-4110-9347-611cd3c45226") id: UUID
  ) {
    LOG.info("Deleting message for id:[$id]")
    deleteKafkaMessageUseCase.invoke(id)
  }

  @ResponseStatus(HttpStatus.NO_CONTENT)
  @Operation(
    summary = "Delete Messages",
    description = "Delete Messages by Kafka Topic and Keys",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "204",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = Unit::class, hidden = true))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "500",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      )
    ]
  )
  @DeleteMapping
  fun deleteMessages(
    @Valid @RequestBody request: ReplayRequest
  ) {
    LOG.info("Deleting messages for topic:[${request.topic}] and keys:[${request.keys}]")
    deleteKafkaMessageUseCase.invoke(request.topic, request.keys)
  }

  @Operation(
    summary = "Get Message",
    description = "Get Message by Replay ID",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "200",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = KafkaReplayMessage::class))
        ]
      ),
      ApiResponse(
        description = "Not found",
        responseCode = "404",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "500",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      )
    ]
  )
  @GetMapping("/{id}")
  fun getMessage(
    @PathVariable("id") @Parameter(description = "Replay ID", example = "10ab1276-663d-4110-9347-611cd3c45226") id: UUID
  ): KafkaReplayMessage {
    LOG.info("Find message for id:[$id]")
    return KafkaReplayV2Converter.convertToResponseDto(getReplayKafkaMessageUseCase.invoke(id))
  }

  @Operation(
    summary = "Get Messages",
    description = "Get Messages by Kafka Topic and Keys",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "200",
        content = [
          Content(mediaType = "application/json", array = ArraySchema(schema = Schema(implementation = KafkaReplayWrapper::class)))
        ]
      ),
      ApiResponse(
        description = "Not found",
        responseCode = "404",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "500",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      )
    ]
  )
  @PostMapping("/search")
  fun getAllMessages(
    @Valid @RequestBody request: ReplayRequest
  ): List<KafkaReplayWrapper> {
    LOG.info("Get all messages for topic:[${request.topic}] and keys:[${request.keys}]")

    return getAllKafkaReplayMessagesUseCase.invoke(request.topic, request.keys).map {
      KafkaReplayWrapper(key = it.key, messages = it.value.map { replay ->  KafkaReplayV2Converter.convertToResponseDto(replay) })
    }
  }

  @Operation(
    summary = "Get List of Topics",
    description = "Get All Topics",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "200",
        content = [
          Content(mediaType = "application/json", array = ArraySchema(schema = Schema(implementation = String::class)))
        ]
      ),
      ApiResponse(
        description = "Not found",
        responseCode = "404",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "500",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      )
    ]
  )
  @GetMapping("/topics")
  fun getAllTopics(): Set<String> {
    LOG.info("Get all Topics")
    return getAllKafkaReplayTopicUseCase.invoke()
  }

  @Operation(
    summary = "Get List of Keys",
    description = "Get All Keys by Topic",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "200",
        content = [
          Content(mediaType = "application/json", array = ArraySchema(schema = Schema(implementation = String::class)))
        ]
      ),
      ApiResponse(
        description = "Not found",
        responseCode = "404",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "500",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      )
    ]
  )
  @GetMapping("/topics/{topic}/keys")
  fun getAllKeys(
    @PathVariable("topic") @Parameter(description = "Kafka Topic", example = "some-topic_ms-kafka-failure_RETRY") topic: String,
  ): Set<String?> {
    LOG.info("Get all Keys for Topic: $topic")
    return getAllKafkaReplayKeyUseCase.invoke(topic)
  }

  @Operation(
    summary = "Replay Message",
    description = "Replay Message by Replay ID",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "204",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = Unit::class, hidden = true))
        ]
      ),
      ApiResponse(
        description = "Not found",
        responseCode = "404",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "500",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Bad Request",
        responseCode = "400",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      )
    ]
  )
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @PostMapping("/retry/{id}")
  fun retryMessage(
    @PathVariable("id") @Parameter(description = "Replay ID", example = "10ab1276-663d-4110-9347-611cd3c45226") id: UUID
  ) {
    LOG.info("Replay message for id:[$id]")
    replayKafkaMessageUseCase.invoke(id)
  }

  @Operation(
    summary = "Replay Messages",
    description = "Replay Messages by Topic and Keys",
    responses = [
      ApiResponse(
        description = "Success",
        responseCode = "204",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = Unit::class, hidden = true))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "400",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      ),
      ApiResponse(
        description = "Internal Error",
        responseCode = "500",
        content = [
          Content(mediaType = "application/json", schema = Schema(implementation = ErrorResponse::class))
        ]
      )
    ]
  )
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @PostMapping("/retry")
  fun retryAllMessages(
    @Valid @RequestBody request: ReplayRequest
  ) {
    LOG.info("Replay all messages for topic:[${request.topic}] and keys:[${request.keys}]")
    replayKafkaMessageUseCase.invoke(request.topic, request.keys)
  }
}