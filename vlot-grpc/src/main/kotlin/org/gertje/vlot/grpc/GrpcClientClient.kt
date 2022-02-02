package org.gertje.vlot.grpc

import io.grpc.Channel
import io.grpc.kotlin.ClientCalls
import kotlinx.serialization.KSerializer
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class GrpcClientClient<T>(
    private val channel: Channel,
    private val serializer: KSerializer<T>,
) {
    suspend fun addMessage(message: T): Boolean =
        try {
            ClientCalls.unaryRpc(
                channel,
                addMessageMethodDescriptor(serializer),
                message,
            )
        } catch (e: Exception) {
            logger.error(e) { "Failed adding a message" }
            false
        }
}
