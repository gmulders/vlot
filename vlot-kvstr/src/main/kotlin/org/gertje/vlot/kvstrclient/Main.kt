package org.gertje.vlot.kvstrclient

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.PairSerializer
import kotlinx.serialization.builtins.serializer
import org.gertje.vlot.grpc.GrpcClientClient
import kotlin.system.measureTimeMillis

fun main() {
    val client = GrpcClientClient(
        ManagedChannelBuilder.forAddress("localhost", 6660).usePlaintext().build(),
        PairSerializer(String.serializer(), Int.serializer()),
    )

    val millis = measureTimeMillis {
        runBlocking {
            repeat(10000) {
                launch {
                    val ret = client.addMessage("wim" to it)
                    println(ret)
                }
            }
        }
    }

    println("time: $millis")
}
