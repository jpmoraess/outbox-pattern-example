package br.com.jpmoraess.outbox_pattern_example.service

import br.com.jpmoraess.outbox_pattern_example.entity.OutboxEvent
import br.com.jpmoraess.outbox_pattern_example.entity.OutboxStatus
import br.com.jpmoraess.outbox_pattern_example.repository.OutboxRepository
import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class OutboxService(
    private val outboxRepository: OutboxRepository,
    private val outboxClaimService: OutboxClaimService,
    private val outboxStatusService: OutboxStatusService,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Publishes pending outbox events to Kafka.
     * This method fetches events that are ready to be processed, sends them to Kafka,
     * and updates their status based on the outcome of the send operation.
     *
     * @param limit The maximum number of events to process in this batch.
     * @param now The current time used for claiming the events.
     */
    fun publishPendingEvents(limit: Int, now: LocalDateTime = LocalDateTime.now()) {
        val claimedEvents = outboxClaimService.fetchAndClaimEvents(limit, now)
        if (claimedEvents.isEmpty()) return

        val futures = claimedEvents.map { event -> sendToKafka(event) }

        try {
            CompletableFuture.allOf(*futures.toTypedArray()).join()
            logger.info("Finished publishing ${futures.size} events.")
        } catch (e: Exception) {
            logger.error("Error while processing outbox events", e)
        }
    }

    /**
     * Sends a single outbox event to Kafka.
     * This method is used to publish an event immediately, typically after it has been created.
     *
     * @param event The OutboxEvent entity to be sent.
     * @return A CompletableFuture that completes when the send operation is done.
     */
    private fun sendToKafka(event: OutboxEvent): CompletableFuture<Void> {
        return CompletableFuture<Void>().also { completion ->
            kafkaTemplate.send("topic", event.aggregateId, event.payload)
                .whenComplete { _, throwable ->
                    try {
                        if (throwable != null) {
                            logger.error("Failed to publish event ${event.aggregateId}", throwable)
                            outboxStatusService.updateStatusSafely(event, OutboxStatus.FAILED)
                        } else {
                            logger.info("Successfully published event ${event.aggregateId}")
                            outboxStatusService.updateStatusSafely(event, OutboxStatus.COMPLETED)
                        }
                    } catch (e: Exception) {
                        logger.error("Error updating status for event ${event.aggregateId}", e)
                    } finally {
                        completion.complete(null)
                    }
                }
        }
    }

    /**
     * Saves a new outbox event to the repository.
     * This method is used to create events that will be processed later.
     *
     * @param aggregateId The ID of the aggregate that this event belongs to.
     * @param eventType The type of the event being saved.
     * @param payload The payload of the event, typically in JSON format.
     * @return The saved OutboxEvent entity.
     */
    @Transactional
    fun saveNewEvent(aggregateId: String, eventType: String, payload: String): OutboxEvent {
        val event = OutboxEvent(aggregateId = aggregateId, eventType = eventType, payload = payload)
        return outboxRepository.save(event)
    }

    /**
     * Releases stuck events that are older than the specified expiration time.
     * This method is useful for cleaning up events that were not processed due to failures.
     *
     * @param expiredBefore The cutoff time for releasing stuck events. Defaults to 3 minutes ago.
     * @return The number of events released.
     */
    @Transactional
    fun releaseStuckEvents(expiredBefore: LocalDateTime = LocalDateTime.now().minusMinutes(3)): Int {
        val count = outboxRepository.releaseStuckEvents(expiredBefore)
        if (count > 0) {
            logger.warn("Released $count stuck events older than $expiredBefore")
        }
        return count
    }
}
