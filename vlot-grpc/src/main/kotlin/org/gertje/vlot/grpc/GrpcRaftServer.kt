package org.gertje.vlot.grpc

import io.grpc.ServerServiceDefinition
import io.grpc.kotlin.AbstractCoroutineServerImpl
import io.grpc.kotlin.ServerCalls.unaryServerMethodDefinition
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import org.gertje.vlot.AddMessageResponse as RaftAddMessageResponse
import org.gertje.vlot.AppendEntriesRequest
import org.gertje.vlot.AppendEntriesResponse
import org.gertje.vlot.ConsensusModule
import org.gertje.vlot.PeerId
import org.gertje.vlot.RequestVoteRequest
import org.gertje.vlot.RequestVoteResponse
import org.gertje.vlot.Success
import org.gertje.vlot.TimeOut
import org.gertje.vlot.WrongLeader
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

class GrpcRaftServer<T>(
    private val consensusModule: ConsensusModule<T>,
    private val serializer: KSerializer<T>,
    coroutineContext: CoroutineContext = EmptyCoroutineContext
): AbstractCoroutineServerImpl(coroutineContext) {

    private suspend fun addMessage(message: T): AddMessageResponse =
        consensusModule.addMessage(message).toResponse()

    private suspend fun appendEntries(request: AppendEntriesRequest<T>): AppendEntriesResponse =
        consensusModule.onAppendEntries(request)

    private suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse =
        consensusModule.onRequestVote(request)

    override fun bindService(): ServerServiceDefinition =
        ServerServiceDefinition
            .builder(SERVICE_NAME)
            .addMethod(
                unaryServerMethodDefinition(
                    context = this.context,
                    descriptor = addMessageMethodDescriptor(serializer),
                    implementation = ::addMessage
                )
            )
            .addMethod(
                unaryServerMethodDefinition(
                    context = this.context,
                    descriptor = appendEntriesMethodDescriptor(serializer),
                    implementation = ::appendEntries
                )
            )
            .addMethod(
                unaryServerMethodDefinition(
                    context = this.context,
                    descriptor = requestVoteMethodDescriptor,
                    implementation = ::requestVote
                )
            )
            .build()
}

enum class AddMessageResult {
    WrongLeader,
    Success,
    Timeout,
    ClientFailure
}

@Serializable
data class AddMessageResponse(
    val state: AddMessageResult,
    val leaderId: PeerId? = null,
)

private fun RaftAddMessageResponse.toResponse(): AddMessageResponse =
    when (this) {
        is WrongLeader -> AddMessageResponse(AddMessageResult.WrongLeader, this.leaderId)
        is Success -> AddMessageResponse(AddMessageResult.Success)
        is TimeOut -> AddMessageResponse(AddMessageResult.Timeout)
    }
