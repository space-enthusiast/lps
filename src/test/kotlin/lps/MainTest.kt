package lps

import org.junit.jupiter.api.Timeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals

@Timeout(value = 3, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class MainTest {
    @Test
    fun `subscriber receives a published message`() {
        val ps = PubSub()
        // widen the race window: the worker now spends a long time iterating broker.values
        for (i in 1..10_000) ps.subscribe("dummy$i") {}
        ps.runLoop()
        // let the worker start iterating so the subscribe below lands mid-iteration
        Thread.sleep(10)
        val seen = mutableListOf<String>()
        ps.subscribe("news") { seen.add(it) }

        ps.publish("news", "hello")

        ps.stop()
        ps.join()

        assertEquals(listOf("hello"), seen)
    }

    @Test
    fun `subscriber receives a published message with drain`() {
        val ps = PubSub()
        val seen = mutableListOf<String>()
        ps.subscribe("news") { seen.add(it) }
        ps.publish("news", "hello")
        ps.drain()

        ps.stop()
        ps.join()

        assertEquals(listOf("hello"), seen)
    }

    @Test
    fun `two subscribers receives a published message`() {
        val ps = PubSub()
        ps.runLoop()
        val seen1 = mutableListOf<String>()
        val seen2 = mutableListOf<String>()
        ps.subscribe("news") { seen1.add(it) }
        ps.subscribe("news") { seen2.add(it) }

        ps.publish("news", "hello")

        ps.stop()
        ps.join()

        assertEquals(listOf("hello"), seen1)
        assertEquals(listOf("hello"), seen2)
    }

    @Test
    fun `a cycle that stops itself runs to completion`() {
        val ps = PubSub()
        ps.runLoop()
        var hops = 0
        ps.subscribe("ping") {
            hops++
            if (hops < 100_000) ps.publish("pong", it)
        }
        ps.subscribe("pong") {
            hops++
            if (hops < 100_000) ps.publish("ping", it) else ps.stop()
        }

        ps.publish("ping", "start")

        ps.join()

        assertEquals(100_000, hops)
    }

    @Test
    fun `stop during a handler still delivers messages already queued`() {
        val ps = PubSub()
        val seen = mutableListOf<String>()
        val inHandler = CountDownLatch(1)
        val release = AtomicBoolean(false)
        ps.subscribe("news") {
            if (it == "first") {
                inHandler.countDown()
                // Not CountDownLatch.await(): an interruptible wait would throw on stop()
                // and take the catch path, hiding the case this test is about.
                while (!release.get()) Thread.onSpinWait()
            }
            seen.add(it)
        }
        ps.runLoop()

        ps.publish("news", "first")
        inHandler.await()               // the worker is now inside the handler for "first"
        ps.publish("news", "second")    // queued while the handler is still running
        ps.stop()                       // interrupt arrives during the handler
        release.set(true)               // let the handler finish
        ps.join()

        assertEquals(listOf("first", "second"), seen)
    }

    @Test
    fun `publish message while handling published messages`() {
        val ps = PubSub()
        val seen = mutableListOf<String>()

        ps.subscribe("news") {
            ps.subscribe("news") {
                seen.add(it)
            }
        }

        ps.runLoop()

        ps.publish("news", "first")
        ps.publish("news", "second")

        ps.stop()
        ps.join()

        assertEquals(listOf("second"), seen)
    }

    @Test
    fun `concurrent first subscribes to the same topic are all kept`() {
        // Stress test: the lost-handler race is rare per topic, so race on 100k fresh topics.
        val ps = PubSub()
        val threads = 4
        val topics = 100_000
        val calls = AtomicInteger()
        val go = CountDownLatch(1)
        val workers = List(threads) {
            thread {
                go.await() // start all threads at the same moment
                for (i in 0 until topics) ps.subscribe("t$i") { calls.incrementAndGet() }
            }
        }
        go.countDown()
        workers.forEach { it.join() }

        for (i in 0 until topics) ps.publish("t$i", "x")
        ps.drain()

        assertEquals(threads * topics, calls.get())
    }
}
