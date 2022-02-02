package org.gertje.vlot.grpc

import io.grpc.Channel
import io.grpc.ManagedChannelBuilder
import io.grpc.kotlin.ClientCalls
import kotlinx.serialization.KSerializer
import mu.KotlinLogging
import org.gertje.vlot.AppendEntriesRequest
import org.gertje.vlot.AppendEntriesResponse
import org.gertje.vlot.PeerId
import org.gertje.vlot.RaftClient
import org.gertje.vlot.RequestVoteRequest
import org.gertje.vlot.RequestVoteResponse

private val logger = KotlinLogging.logger {}

class GrpcRaftClient<T>(
    peerUrls: Map<PeerId, Pair<String, Int>>,
    private val serializer: KSerializer<T>,
): RaftClient<T> {

    private val channels: Map<PeerId, Channel> = peerUrls
        .mapValues { (_, pair) ->
            ManagedChannelBuilder
                .forAddress(pair.first, pair.second)
                .usePlaintext()
                .build()
        }

    override suspend fun appendEntries(peerId: PeerId, request: AppendEntriesRequest<T>): AppendEntriesResponse =
        try {
            ClientCalls.unaryRpc(
                channels[peerId]!!,
                appendEntriesMethodDescriptor(serializer),
                request
            )
        } catch (e: Exception) {
            logger.error(e) { "Failed appending entries" }
            AppendEntriesResponse(0, false)
        }

    override suspend fun requestVote(peerId: PeerId, request: RequestVoteRequest): RequestVoteResponse =
        try {
            ClientCalls.unaryRpc(
                channels[peerId]!!,
                requestVoteMethodDescriptor,
                request,
            )
        } catch (e: Exception) {
            logger.error(e) { "Failed requesting vote" }
            RequestVoteResponse(0, false)
        }
}
