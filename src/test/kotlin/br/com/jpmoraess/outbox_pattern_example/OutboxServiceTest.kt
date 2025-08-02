package br.com.jpmoraess.outbox_pattern_example

import br.com.jpmoraess.outbox_pattern_example.entity.OutboxEvent
import br.com.jpmoraess.outbox_pattern_example.entity.OutboxStatus
import br.com.jpmoraess.outbox_pattern_example.repository.OutboxRepository
import br.com.jpmoraess.outbox_pattern_example.service.OutboxService
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.CompletableFuture
import org.apache.kafka.clients.producer.RecordMetadata
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.data.domain.PageRequest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.orm.ObjectOptimisticLockingFailureException

@ExtendWith(MockitoExtension::class)
class OutboxServiceTest {

    @Mock
    lateinit var repository: OutboxRepository

    @Mock
    lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @InjectMocks
    lateinit var outboxService: OutboxService

    @Test
    fun `publishPendingEvents should log and return when no events`() {
        `when`(repository.findClaimableIds(PageRequest.of(0, 10)))
            .thenReturn(emptyList())

        outboxService.publishPendingEvents(limit = 10)

        verify(repository, times(1)).findClaimableIds(PageRequest.of(0, 10))
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString())
    }

    @Test
    fun `publishPendingEvents should send to kafka and update COMPLETED on success`() {
        val event = OutboxEvent(
            id = 1L,
            aggregateId = UUID.randomUUID().toString(),
            eventType = "type",
            payload = "payload",
            status = OutboxStatus.STARTED,
            claimedAt = null,
        )
        val now = LocalDateTime.now()
        `when`(repository.findClaimableIds(PageRequest.of(0, 10)))
            .thenReturn(listOf(1L))
        `when`(repository.claimEvents(now, listOf(1L)))
            .thenReturn(1)
        `when`(repository.findByIds(listOf(1L)))
            .thenReturn(listOf(event))
        val sendResult = SendResult<String, String>(null, RecordMetadata(null, 0, 0, 0, 0L, 0, 0))
        val future = CompletableFuture.completedFuture(sendResult)
        `when`(kafkaTemplate.send("topic", event.aggregateId, event.payload))
            .thenReturn(future)
        `when`(repository.save(any(OutboxEvent::class.java)))
            .thenAnswer { it.getArgument(0) }

        outboxService.publishPendingEvents(limit = 10, now = now)

        verify(kafkaTemplate).send("topic", event.aggregateId, event.payload)
        verify(repository).save(event)
        assertEquals(OutboxStatus.COMPLETED, event.status)
        assertNull(event.claimedAt)
    }

    @Test
    fun `publishPendingEvents should update FAILED on kafka send error`() {
        val event = OutboxEvent(
            id = 1L,
            aggregateId = "id",
            eventType = "type",
            payload = "payload",
            status = OutboxStatus.STARTED,
            claimedAt = null
        )
        val now = LocalDateTime.now()
        `when`(repository.findClaimableIds(PageRequest.of(0, 10)))
            .thenReturn(listOf(1L))
        `when`(repository.claimEvents(now, listOf(1L)))
            .thenReturn(1)
        `when`(repository.findByIds(listOf(1L)))
            .thenReturn(listOf(event))
        val future = CompletableFuture<SendResult<String, String>>()
        future.completeExceptionally(RuntimeException("Kafka error"))
        `when`(kafkaTemplate.send("topic", event.aggregateId, event.payload))
            .thenReturn(future)
        `when`(repository.save(event))
            .thenAnswer { it.getArgument(0) }

        outboxService.publishPendingEvents(limit = 10, now = now)

        verify(kafkaTemplate).send("topic", event.aggregateId, event.payload)
        verify(repository).save(event)
        assertEquals(OutboxStatus.FAILED, event.status)
        assertNull(event.claimedAt)
    }

    @Test
    fun `updateStatusSafely should log and ignore on ObjectOptimisticLockingFailureException`() {
        val event = OutboxEvent(
            id = 1L,
            aggregateId = UUID.randomUUID().toString(),
            eventType = "type",
            payload = "payload",
            status = OutboxStatus.PROCESSING
        )
        doThrow(ObjectOptimisticLockingFailureException::class.java)
            .`when`(repository).save(event)

        outboxService.updateStatusSafely(event, OutboxStatus.COMPLETED)

        verify(repository).save(event)
        assertEquals(OutboxStatus.COMPLETED, event.status)
    }

    @Test
    fun `updateStatusSafely should not throw on general exception`() {
        val event = OutboxEvent(
            id = 1L,
            aggregateId = UUID.randomUUID().toString(),
            eventType = "type",
            payload = "payload",
            status = OutboxStatus.PROCESSING
        )
        doThrow(RuntimeException("Some DB error")).`when`(repository).save(event)

        assertThrows<RuntimeException> {
            outboxService.updateStatusSafely(event, OutboxStatus.COMPLETED)
        }

        verify(repository).save(event)
    }
}
