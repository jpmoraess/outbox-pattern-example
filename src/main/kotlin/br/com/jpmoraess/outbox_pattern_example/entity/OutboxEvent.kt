package br.com.jpmoraess.outbox_pattern_example.entity

import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Version
import java.time.LocalDateTime
import java.time.ZoneId

@Entity
data class OutboxEvent(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0L,
    val aggregateId: String,
    val eventType: String,
    val payload: String,
    @Enumerated(EnumType.STRING)
    var status: OutboxStatus = OutboxStatus.STARTED,
    @Version
    val version: Int = 0,
    val createdAt: LocalDateTime = LocalDateTime.now(ZoneId.of("UTC")),
    var claimedAt: LocalDateTime? = null
)
