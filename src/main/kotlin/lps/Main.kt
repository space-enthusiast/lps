package lps

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

class PubSub {
    data class Message(val message: String, val topic: String)

    private val topicToSubscribers: ConcurrentHashMap<String, CopyOnWriteArrayList<(String) -> Unit>> = ConcurrentHashMap()
    private val broker: LinkedBlockingQueue<Message> = LinkedBlockingQueue<Message>()

    fun subscribe(topic: String, handler: (String) -> Unit) {
        topicToSubscribers.computeIfAbsent(topic) { CopyOnWriteArrayList() }.add(handler)
    }

    fun publish(topic: String, message: String) {
        broker.add(Message(topic, message))
    }

    private var eventLoop: Thread? = null
    fun runLoop() {
        eventLoop = thread(name = "EventLoop") {
            try {
                loop()
            } catch (_: InterruptedException) {
                drain()
            }
        }
    }

    fun stop() {
        eventLoop?.interrupt()
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

    fun take(): List<Message> {
        val messages: MutableList<Message> = mutableListOf()
        val message = broker.take() // stops the thread if there is no message in queue
        messages.add(message)
        while (broker.isNotEmpty()) {
            messages.add(broker.poll())
        }
        return messages
    }

    fun loop() {
        while (broker.isNotEmpty() || Thread.currentThread().isInterrupted.not()) {
            val messages: List<Message> = take()
            for ((topic, message) in messages) {
                val subscribers = topicToSubscribers[topic] ?: continue
                for (subscriber in subscribers) {
                    subscriber(message)
                }
            }
        }
    }

    fun join() {
        eventLoop?.join()
    }
}

fun main() { }