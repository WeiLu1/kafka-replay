package com.kafkareplay.config.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.converter.StringJsonMessageConverter


@Configuration
@EnableKafka
class KafkaListenerConfig(
  @Value("\${spring.kafka.bootstrap-servers}") server: String
) {

  val consumerProps = mapOf(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
    ConsumerConfig.GROUP_ID_CONFIG to "group",
//    KEY_DESERIALIZER_CLASS to StringDeserializer::class.java,
//    VALUE_DESERIALIZER_CLASS to StringDeserializer::class.java,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
  )

  @Bean
  fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<Int, String>  {
    val listenerContainerFactory = ConcurrentKafkaListenerContainerFactory<Int, String>().also {
      it.consumerFactory = consumerFactory()
      it.setMessageConverter(StringJsonMessageConverter())
    }

    listenerContainerFactory.containerProperties.isObservationEnabled = true
    return listenerContainerFactory
  }


  fun consumerFactory() = DefaultKafkaConsumerFactory<Int, String>(consumerProps)
}
