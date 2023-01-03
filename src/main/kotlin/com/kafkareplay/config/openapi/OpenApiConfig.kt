package com.kafkareplay.config.openapi

import io.swagger.v3.oas.models.Components
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Contact
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.info.License
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class OpenApiConfig {
  @Bean
  fun customOpenAPI(): OpenAPI {
    return OpenAPI()
      .components(Components())
      .info(
        Info()
          .title("Kafka Replay API")
          .description("Endpoints that would allow you to replay messages using Kafka")
          .contact(Contact().email("anojkunes@googlemail.com"))
          .version("2.0")
      )
  }
}