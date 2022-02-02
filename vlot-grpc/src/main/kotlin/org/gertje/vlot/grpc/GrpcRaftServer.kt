package org.gertje.vlot.grpc

import io.grpc.ServerServiceDefinition
import io.grpc.kotlin.AbstractCoroutineServerImpl
import io.grpc.kotlin.ServerCalls
import kotlinx.serialization.KSerializer
import org.gertje.vlot.AppendEntriesRequest
import org.gertje.vlot.AppendEntriesResponse
import org.gertje.vlot.ConsensusModule
import org.gertje.vlot.RequestVoteRequest
import org.gertje.vlot.RequestVoteResponse
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

class GrpcRaftServer<T>(
    private val consensusModule: ConsensusModule<T>,
    private val serializer: KSerializer<T>,
    coroutineContext: CoroutineContext = EmptyCoroutineContext
): AbstractCoroutineServerImpl(coroutineContext) {

    private suspend fun addMessage(message: T): Boolean =
        consensusModule.addMessage(message)

    private suspend fun appendEntries(request: AppendEntriesRequest<T>): AppendEntriesResponse =
        consensusModule.onAppendEntries(request)

    private suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse =
        consensusModule.onRequestVote(request)

    override fun bindService(): ServerServiceDefinition =
        ServerServiceDefinition
            .builder(SERVICE_NAME)
            .addMethod(
                ServerCalls.unaryServerMethodDefinition(
                    context = this.context,
                    descriptor = addMessageMethodDescriptor(serializer),
                    implementation = ::addMessage
                )
            )
            .addMethod(
                ServerCalls.unaryServerMethodDefinition(
                    context = this.context,
                    descriptor = appendEntriesMethodDescriptor(serializer),
                    implementation = ::appendEntries
                )
            )
            .addMethod(
                ServerCalls.unaryServerMethodDefinition(
                    context = this.context,
                    descriptor = requestVoteMethodDescriptor,
                    implementation = ::requestVote
                )
            )
            .build()
}
