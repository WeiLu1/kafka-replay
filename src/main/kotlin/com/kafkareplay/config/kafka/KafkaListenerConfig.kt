package com.kafkareplay.config.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.retry.backoff.ExponentialBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate

@Configuration
@EnableKafka
class KafkaListenerConfig(
  @Value("\${spring.kafka.bootstrap-servers}") server: String
) {

  val consumerProps = mapOf(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
    ConsumerConfig.GROUP_ID_CONFIG to "group",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
  )

  @Primary
  @Bean
  fun kafkaListenerContainerFactory(consumerFactory: ConsumerFactory<Int, String>) =
    ConcurrentKafkaListenerContainerFactory<Int, String>().also {
      it.consumerFactory = consumerFactory
    }

  @Bean
  fun consumerFactory() = DefaultKafkaConsumerFactory<Int, String>(consumerProps)
}
