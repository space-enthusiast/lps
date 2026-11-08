package lps

import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.Volatile
import kotlin.concurrent.thread

class PubSub {
    data class Message(val message: String, val topic: String)

    private val topicToSubscribers: MutableMap<String, MutableList<(String) -> Unit>> = mutableMapOf()
    private val broker: LinkedBlockingQueue<Message> = LinkedBlockingQueue<Message>()

    fun subscribe(topic: String, handler: (String) -> Unit) {
        val subscribers = topicToSubscribers[topic]
        if (subscribers != null) {
            subscribers.add(handler)
        } else {
            topicToSubscribers[topic] = mutableListOf(handler)
        }
    }

    fun publish(topic: String, message: String) {
        broker.add(Message(topic, message))
    }

    private var eventLoop: Thread? = null
    @Volatile
    private var eventLoopOn: Boolean = true
    fun runLoop() {
        eventLoop = thread(name = "EventLoop") {
            loop()
        }
    }

    fun stop() {
        while (true) {
            if (broker.isEmpty()) {
                break
            }
        }
        eventLoopOn = false
        join()
    }

    fun drain() {
        while (broker.isNotEmpty()) {
            val (topic, message) = broker.poll()
            val subscribers = topicToSubscribers[topic] ?: emptyList()
            for (subscriber in subscribers) {
                subscriber(message)
            }
        }
    }

    fun loop() {
        while (eventLoopOn) {
            drain()
        }
    }

    fun join() {
        eventLoop?.join()
    }
}

fun main() {

    val worker = thread(name = "worker") {
        println("Running on ${Thread.currentThread().name}")
    }

    println("main continues without waiting")

    worker.join()
}