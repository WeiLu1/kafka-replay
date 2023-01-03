package com.kafkareplay.adapter.driven.repository.kafkareplay.mongo

import com.kafkareplay.domain.OriginalPositionReference

object PositionReferenceMapper {
  fun toEntity(originalPositionReference: OriginalPositionReference): PositionReferenceEntity {
    return PositionReferenceEntity(
      originalPositionReference.partition,
      originalPositionReference.offset,
      originalPositionReference.topic,
      originalPositionReference.order
    )
  }

  fun toDomain(originalPositionReference: PositionReferenceEntity): OriginalPositionReference {
    return OriginalPositionReference(
      originalPositionReference.partition,
      originalPositionReference.offset,
      originalPositionReference.topic,
      originalPositionReference.order
    )
  }
}