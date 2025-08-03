package br.com.jpmoraess.outbox_pattern_example.repository

import br.com.jpmoraess.outbox_pattern_example.entity.OutboxEvent
import br.com.jpmoraess.outbox_pattern_example.entity.OutboxStatus
import java.time.LocalDateTime
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

@Repository
interface OutboxRepository : JpaRepository<OutboxEvent, Long> {

    @Query("SELECT e FROM OutboxEvent e WHERE e.id IN :ids")
    fun findByIds(@Param("ids") ids: List<Long>): List<OutboxEvent>

    @Query(
        """
        SELECT id FROM outbox_event
        WHERE status IN ('STARTED', 'FAILED')
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    """,
        nativeQuery = true
    )
    fun findAndLockClaimableIds(@Param("limit") limit: Int): List<Long>

    @Modifying
    @Query(
        """
        UPDATE OutboxEvent e
        SET e.status = 'PROCESSING',
            e.claimedAt = :now
        WHERE e.id IN :ids AND e.status IN ('STARTED', 'FAILED')
    """
    )
    fun claimEvents(
        @Param("now") now: LocalDateTime,
        @Param("ids") ids: List<Long>
    ): Int

    @Modifying
    @Query(
        """
        UPDATE OutboxEvent e
        SET e.status = 'FAILED',
            e.claimedAt = NULL
        WHERE e.status = 'PROCESSING' AND e.claimedAt < :expiredBefore
    """
    )
    fun releaseStuckEvents(@Param("expiredBefore") expiredBefore: LocalDateTime): Int

    @Modifying
    @Query("UPDATE OutboxEvent e SET e.status = :status, e.claimedAt = null WHERE e.id = :id")
    fun updateStatusById(@Param("id") id: Long, @Param("status") status: OutboxStatus): Int
}
