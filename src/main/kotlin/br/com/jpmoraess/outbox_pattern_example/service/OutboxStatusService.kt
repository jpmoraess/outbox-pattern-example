package br.com.jpmoraess.outbox_pattern_example.service

import br.com.jpmoraess.outbox_pattern_example.entity.OutboxEvent
import br.com.jpmoraess.outbox_pattern_example.entity.OutboxStatus
import br.com.jpmoraess.outbox_pattern_example.repository.OutboxRepository
import org.slf4j.LoggerFactory
import org.springframework.orm.ObjectOptimisticLockingFailureException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class OutboxStatusService(
    private val outboxRepository: OutboxRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Updates the status of an outbox event safely, handling optimistic locking exceptions.
     * This method is used to change the status of an event after processing it.
     *
     * @param event The OutboxEvent entity to update.
     * @param status The new status to set for the event.
     */
    @Transactional
    fun updateStatusSafely(event: OutboxEvent, status: OutboxStatus) {
        try {
            val updatedRows = outboxRepository.updateStatusById(event.id, status)
            if (updatedRows == 0) {
                logger.warn("No rows updated for event ${event.aggregateId}. It might have been updated concurrently.")
            }
        } catch (_: ObjectOptimisticLockingFailureException) {
            logger.warn("Optimistic locking conflict on event ${event.aggregateId}. Ignoring.")
        } catch (e: Exception) {
            logger.error("Failed to update event ${event.aggregateId} to $status: ${e.message}", e)
        }
    }
}
