package br.com.jpmoraess.outbox_pattern_example.entity

enum class OutboxStatus {
    STARTED,
    PROCESSING,
    COMPLETED,
    FAILED;
}