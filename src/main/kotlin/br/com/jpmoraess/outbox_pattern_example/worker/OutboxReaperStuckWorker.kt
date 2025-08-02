package br.com.jpmoraess.outbox_pattern_example.worker

import br.com.jpmoraess.outbox_pattern_example.repository.OutboxRepository
import br.com.jpmoraess.outbox_pattern_example.service.OutboxService
import java.time.LocalDateTime
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class OutboxReaperStuckWorker(
    private val outboxService: OutboxService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelay = 60000L) // Run every 60 seconds
    fun run() {
        logger.info("Running Outbox Reaper Stuck Worker...")
        outboxService.releaseStuckEvents()
    }
}
