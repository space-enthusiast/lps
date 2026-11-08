package lps

import kotlin.concurrent.thread

class PubSub {
    private val broker = mutableMapOf<String, Topic>()

    private fun messageExist(): Boolean {
        val brokerQueues: List<ArrayDeque<String>> = broker.values.map { it.queue }
        for (queue in brokerQueues) {
            if (queue.isNotEmpty()) { return true }
        }
        return false
    }

    class Topic {
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
    private var eventLoopOn: Boolean = true
    fun runLoop() {
        eventLoop = thread(name = "EventLoop") {
            while (eventLoopOn) {
                while (messageExist()) {
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
    }

    fun waitForRunLoopStop() {
        while (true) {
            if (messageExist().not()) {
                break
            }
        }
        eventLoopOn = false
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