package br.com.jpmoraess.outbox_pattern_example.controller

import br.com.jpmoraess.outbox_pattern_example.service.OutboxService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/outbox")
class OutboxController(
    private val outboxService: OutboxService
) {

    @PostMapping("/events")
    fun save(@RequestBody request: OutboxEventRequest) {
        outboxService.saveNewEvent(
            aggregateId = request.aggregateId,
            eventType = "ORDER_CREATED",
            payload = request.payload
        )
    }

    data class OutboxEventRequest(
        val aggregateId: String,
        val payload: String
    )
}
