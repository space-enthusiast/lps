package lps

class PubSub {
    private val subscribers = mutableMapOf<String, MutableList<(String) -> Unit>>()

    fun subscribe(topic: String, handler: (String) -> Unit) {
        if(subscribers[topic] == null){
            subscribers[topic] = mutableListOf(handler)
        } else {
            subscribers[topic]?.add(handler)
        }
    }

    fun publish(topic: String, message: String) {
        if(subscribers[topic] != null){
            for(handler in subscribers[topic]!!){
                handler(message)
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
}