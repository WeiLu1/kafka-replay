package com.kafkareplay.adapter.driven.repository.kafkareplay

import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.KafkaReplayMapper
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.KafkaReplayMongoRepository
import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.domain.KafkaReplay
import java.util.*
import org.springframework.data.domain.Sort

class KafkaReplayRepository(
  private val mongoRepository: KafkaReplayMongoRepository
): IKafkaReplayRepository {
  override fun save(kafkaReplay: KafkaReplay): KafkaReplay {
    val entity = KafkaReplayMapper.toEntity(kafkaReplay)
    return mongoRepository.save(entity).let {
      KafkaReplayMapper.toDomain(it)
    }
  }

  override fun getAll(): List<KafkaReplay> {
    return mongoRepository.findAll().let {
      it.map { KafkaReplayMapper.toDomain(it) }
    }
  }

  override fun getById(id: UUID): KafkaReplay? {
    return mongoRepository
      .findById(id)
      .map { KafkaReplayMapper.toDomain(it) }
      .orElse(null)
  }

  override fun getAllByTopic(topic: String): List<KafkaReplay> {
    return mongoRepository.findAllByTopic(topic).let {
      it.map { KafkaReplayMapper.toDomain(it) }
    }
  }

  override fun getAllByTopicAndKeys(topic: String, key: String?): List<KafkaReplay> {
    return mongoRepository.findAllByTopicAndKey(
      topic,
      key,
      Sort.by(
        Sort.Order(Sort.Direction.ASC, "originalPositionReference.partition"),
        Sort.Order(Sort.Direction.ASC, "originalPositionReference.offset")
      )
    ).let {
      it.map { entity -> KafkaReplayMapper.toDomain(entity) }
    }
  }

  override fun getPreviousReplayMessage(key: String?, topic: String, partition: Int, offset: Long): KafkaReplay? {
    return mongoRepository.findFirstByKeyAndTopicAndOriginalPositionReference_PartitionAndOriginalPositionReference_OffsetLessThan(
      key = key,
      topic = topic,
      partition = partition,
      offset = offset,
      Sort.by(
        Sort.Order(Sort.Direction.ASC, "originalPositionReference.partition"),
        Sort.Order(Sort.Direction.DESC, "originalPositionReference.offset")
      )
    )?.let {
      KafkaReplayMapper.toDomain(it)
    }
  }

  override fun deleteById(id: UUID) {
    mongoRepository.deleteById(id)
  }

  override fun deleteByTopicAndKey(topic: String, key: String?) {
    mongoRepository.deleteAllByTopicAndKey(topic, key)
  }
}