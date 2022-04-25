package org.gertje.vlot

import kotlinx.serialization.Serializable

interface RaftClient<T> {
    suspend fun appendEntries(peerId: PeerId, request: AppendEntriesRequest<T>): AppendEntriesResponse
    suspend fun requestVote(peerId: PeerId, request: RequestVoteRequest): RequestVoteResponse
}

@Serializable
data class AppendEntriesRequest<T>(
    val term: Term,
    val leaderId: PeerId,
    val prevLogIndex: Long,
    val prevLogTerm: Term,
    val entries: List<Entry<T>>,
    val leaderCommit: Long,
)

@Serializable
data class Entry<T>(
    val term: Term,
    val value: T,
)

@Serializable
data class AppendEntriesResponse(
    val term: Long,
    val success: Boolean,
)

@Serializable
data class RequestVoteRequest(
    val term: Long,
    val candidateId: Int,
    val lastLogIndex: Long,
    val lastLogTerm: Long,
)

@Serializable
data class RequestVoteResponse(
    val term: Long,
    val voteGranted: Boolean,
)

sealed interface AddMessageResponse
object Success : AddMessageResponse
object TimeOut : AddMessageResponse
data class WrongLeader(
    val leaderId: PeerId,
) : AddMessageResponse
