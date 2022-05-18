package com.kafkareplay

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaReplayApplication

fun main(args: Array<String>) {
	runApplication<KafkaReplayApplication>(*args)
}
