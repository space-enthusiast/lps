package lps

import kotlin.test.Test
import kotlin.test.assertEquals

class MainTest {
    @Test
    fun `subscriber receives a published message`() {
        val ps = PubSub()
        ps.runLoop()
        val seen = mutableListOf<String>()
        ps.subscribe("news") { seen.add(it) }

        ps.publish("news", "hello")

        ps.waitForRunLoopStop()

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

        ps.waitForRunLoopStop()

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
            if (hops < 100_000) ps.publish("ping", it)
        }

        ps.publish("ping", "start")

        ps.waitForRunLoopStop()

        assertEquals(100_000, hops)
    }
}
