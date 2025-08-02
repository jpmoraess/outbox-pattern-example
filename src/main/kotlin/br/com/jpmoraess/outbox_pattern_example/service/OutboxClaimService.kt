package br.com.jpmoraess.outbox_pattern_example.service

import br.com.jpmoraess.outbox_pattern_example.entity.OutboxEvent
import br.com.jpmoraess.outbox_pattern_example.repository.OutboxRepository
import jakarta.persistence.EntityManager
import jakarta.persistence.PersistenceContext
import java.time.LocalDateTime
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class OutboxClaimService(
    private val outboxRepository: OutboxRepository,
    @PersistenceContext private val entityManager: EntityManager
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Fetches and claims a batch of outbox events that are ready to be processed.
     * The events are locked for processing to prevent concurrent claims.
     *
     * @param limit The maximum number of events to fetch and claim.
     * @param now The current time used for claiming the events.
     * @return A list of claimed outbox events.
     */
    @Transactional
    fun fetchAndClaimEvents(limit: Int = 10, now: LocalDateTime = LocalDateTime.now()): List<OutboxEvent> {
        val ids = outboxRepository.findAndLockClaimableIds(limit)
        if (ids.isEmpty()) {
            logger.debug("No claimable events found.")
            return emptyList()
        }
        val updatedCount = outboxRepository.claimEvents(now, ids)
        if (updatedCount == 0) {
            logger.debug("No events were claimed (possibly claimed by another instance).")
            return emptyList()
        }
        entityManager.flush()
        entityManager.clear()
        return outboxRepository.findByIds(ids)
    }
}