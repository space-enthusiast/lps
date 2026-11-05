package lps

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

    fun runLoop() {
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

fun main() {
    val ps = PubSub()

    ps.subscribe("news") { println("A got: $it") }
    ps.subscribe("news") { println("B got: $it") }
    ps.subscribe("sports") { println("C got: $it") }

    ps.publish("news", "hello")
    ps.publish("sports", "goal")
    ps.publish("nobody", "lost")

    ps.subscribe("news") { ps.publish("news", it) }
    ps.publish("news", "hello2")
}