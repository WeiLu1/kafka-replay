package com.kafkareplay.commons.exception

import io.micrometer.tracing.Tracer
import jakarta.validation.ConstraintViolationException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.validation.BindingResult
import org.springframework.validation.FieldError
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.MissingServletRequestParameterException
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import java.util.stream.Collectors
import mu.KotlinLogging
import kotlin.Exception
import kotlin.RuntimeException
import kotlin.String

import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException


@ControllerAdvice
class ExceptionHandler(
  val tracer: Tracer
) {
  companion object {
    private val LOG = KotlinLogging.logger {}
  }

  @ExceptionHandler(KafkaReplayBaseException::class)
  fun handleKafkaReplayBaseException(e: KafkaReplayBaseException): ResponseEntity<ErrorResponse> {
    return createResponse(
      e.httpStatus,
      ErrorResponse(e.error.errorCode, e.message, getTracerId())
    )
  }

  @ExceptionHandler(MethodArgumentNotValidException::class)
  fun methodArgumentNotValidException(e: MethodArgumentNotValidException): ResponseEntity<ErrorResponse> {
    val response = ErrorResponse(KafkaReplayError.VALIDATION_ERROR.errorCode, getBeanValidationError(e.bindingResult), getTracerId())
    return createResponse(HttpStatus.BAD_REQUEST, response)
  }

  @ExceptionHandler(HttpMessageNotReadableException::class)
  fun httpMessageNotReadableException(e: HttpMessageNotReadableException): ResponseEntity<ErrorResponse> {
    LOG.error("Malformed Json Parse Exception", e)
    return createResponse(HttpStatus.INTERNAL_SERVER_ERROR, KafkaReplayError.MALFORMED_JSON_ERROR, e.message)
  }

  @ExceptionHandler(
    ConstraintViolationException::class,
    MissingServletRequestParameterException::class,
    MethodArgumentTypeMismatchException::class
  )
  fun handleConstraintViolationException(e: Exception): ResponseEntity<ErrorResponse> {
    return createResponse(HttpStatus.BAD_REQUEST, KafkaReplayError.VALIDATION_ERROR, e.message)
  }

  @ExceptionHandler(RuntimeException::class)
  fun unexpectedError(e: Exception): ResponseEntity<ErrorResponse> {
    LOG.error("Unhandled error: ${e.message}", e)
    return createResponse(HttpStatus.INTERNAL_SERVER_ERROR, KafkaReplayError.INTERNAL_ERROR, e.message)
  }

  private fun createResponse(status: HttpStatus, error: KafkaReplayError, vararg args: String?): ResponseEntity<ErrorResponse> {
    return createResponse(status, ErrorResponse(error.errorCode, error.errorMessage.format(*args), getTracerId()))
  }

  private fun createResponse(status: HttpStatus, response: ErrorResponse): ResponseEntity<ErrorResponse> {
    return ResponseEntity.status(status).body(response)
  }

  private fun getBeanValidationError(binding: BindingResult): String {
    return binding.fieldErrors.stream().map { m: FieldError -> m.field + " " + m.defaultMessage }
      .collect(Collectors.joining(", "))
  }

  private fun getTracerId() = tracer.currentSpan()?.context()?.traceId() +
      "_" +
      tracer.currentSpan()?.context()?.spanId()

}
