package org.gertje.vlot.kvstr

fun main() {
    val id = System.getenv("id")
        ?.toInt()
        ?: throw IllegalStateException("Missing id property")
    val port = System.getenv("port")
        ?.toInt()
        ?: throw IllegalStateException("Missing port property")
    val peerUrls = System.getenv("peer_urls")
        ?.split(",")
        ?.associate {
            val x = it.split("=")
            val x1 = x[1].split(":")
            x[0].toInt() to Pair(x1[0], x1[1].toInt())
        }
        ?: throw IllegalStateException("Missing peerUrls property")

    println("Starting with: $id, $port, $peerUrls")
    Node(id, port, peerUrls).start()
}
