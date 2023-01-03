package com.kafkareplay.commons.helper

import java.util.*
import mu.KotlinLogging

object Base64Converter {
  private val LOG = KotlinLogging.logger {}

  fun convertToBase64(value: String): String {
    val converted = Base64.getEncoder().encodeToString(value.toByteArray())
    LOG.info("encoded = $value & converted = $converted")
    return converted
  }

  fun decodeBase64(encoded: String): String {
    val decodedBytes = Base64.getDecoder().decode(encoded)
    val decoded = String(decodedBytes)
    LOG.info("encoded = $encoded & decoded = $decoded")
    return decoded
  }
}