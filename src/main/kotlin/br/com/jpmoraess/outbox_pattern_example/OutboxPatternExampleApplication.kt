package br.com.jpmoraess.outbox_pattern_example

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
class OutboxPatternExampleApplication

fun main(args: Array<String>) {
	runApplication<OutboxPatternExampleApplication>(*args)
}
