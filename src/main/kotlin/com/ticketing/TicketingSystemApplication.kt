package com.ticketing

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class TicketingSystemApplication

fun main(args: Array<String>) {
    runApplication<TicketingSystemApplication>(*args)
}

