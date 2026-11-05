package lps

import kotlin.concurrent.thread

class PubSub {
    private val broker = mutableMapOf<String, Topic>()

    private class Topic {
        val queue = ArrayDeque<String>()
        val subscribers = mutableListOf<(String) -> Unit>()
    }

    fun subscribe(topic: String, handler: (String) -> Unit) {
        val t = broker[topic]
        if(t == null){
            broker[topic] = Topic().also { it.subscribers.add(handler) }
        } else {
            t.subscribers.add(handler)
        }
    }

    fun publish(topic: String, message: String) {
        val t = broker[topic] ?: return
        t.queue.add(message)
    }

    private var eventLoop: Thread? = null
    fun runLoop() {
        eventLoop = thread(name = "EventLoop") {
            while (broker.values.any { it.queue.isNotEmpty() }) {
                for (topic in broker.values) {
                    val queue = topic.queue
                    while (queue.isNotEmpty()) {
                        val poppedMessage = queue.removeFirst()
                        for (subscriber in topic.subscribers) {
                            subscriber(poppedMessage)
                        }
                    }
                }
            }
        }
    }

    fun waitForRunLoopStop() {
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