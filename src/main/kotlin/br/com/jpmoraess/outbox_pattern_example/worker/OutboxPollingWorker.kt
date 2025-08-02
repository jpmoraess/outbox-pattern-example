package br.com.jpmoraess.outbox_pattern_example.worker

import br.com.jpmoraess.outbox_pattern_example.service.OutboxService
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class OutboxPollingWorker(
    private val outboxService: OutboxService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 5000) // Poll every 5 seconds
    fun run() {
        logger.info("Starting Outbox Polling Worker...")
        outboxService.publishPendingEvents(limit = 10)
    }
}