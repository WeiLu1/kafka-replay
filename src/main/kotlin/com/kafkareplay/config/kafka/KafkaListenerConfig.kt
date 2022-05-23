package com.kafkareplay.config.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.converter.StringJsonMessageConverter
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS


@Configuration
@EnableKafka
class KafkaListenerConfig(
  @Value("\${spring.kafka.bootstrap-servers}") server: String
) {

  val consumerProps = mapOf(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
    ConsumerConfig.GROUP_ID_CONFIG to "group",
    KEY_DESERIALIZER_CLASS to StringDeserializer::class.java,
    VALUE_DESERIALIZER_CLASS to StringDeserializer::class.java,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
  )

  @Bean
  fun kafkaListenerContainerFactory(consumerFactory: ConsumerFactory<Int, String>) =
    ConcurrentKafkaListenerContainerFactory<Int, String>().also {
      it.consumerFactory = consumerFactory()
      it.setMessageConverter(StringJsonMessageConverter())
    }

  fun consumerFactory() = DefaultKafkaConsumerFactory<Int, String>(consumerProps)
}
