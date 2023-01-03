package com.kafkareplay.application.port.adapter.driven.event.sender

interface IRetryTopicSender {
  fun send(topic: String, key: String?, data: String, headers: Map<String, Any> = emptyMap())
}