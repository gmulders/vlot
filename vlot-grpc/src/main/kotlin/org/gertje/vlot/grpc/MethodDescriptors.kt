package org.gertje.vlot.grpc

import io.grpc.MethodDescriptor
import io.grpc.MethodDescriptor.MethodType.UNARY
import kotlinx.serialization.KSerializer
import org.gertje.vlot.AppendEntriesResponse
import org.gertje.vlot.RequestVoteRequest
import org.gertje.vlot.RequestVoteResponse

internal const val SERVICE_NAME = "org.gertje.vlot.grpc"

internal fun <T> appendEntriesMethodDescriptor(serializer: KSerializer<T>) =
    MethodDescriptor
        .newBuilder(
            appendEntriesRequestMarshallerFor(serializer),
            marshallerFor(AppendEntriesResponse.serializer()),
        )
        .setFullMethodName(
            MethodDescriptor.generateFullMethodName(SERVICE_NAME, "appendEntries")
        )
        .setType(UNARY)
        .build()!!

internal fun <T> addMessageMethodDescriptor(serializer: KSerializer<T>) =
    MethodDescriptor
        .newBuilder(
            marshallerFor(serializer),
            marshallerFor(AddMessageResponse.serializer()),
        )
        .setFullMethodName(
            MethodDescriptor.generateFullMethodName(SERVICE_NAME, "addMessage")
        )
        .setType(UNARY)
        .build()!!

internal val requestVoteMethodDescriptor =
    MethodDescriptor
        .newBuilder(
            marshallerFor(RequestVoteRequest.serializer()),
            marshallerFor(RequestVoteResponse.serializer()),
        )
        .setFullMethodName(
            MethodDescriptor.generateFullMethodName(SERVICE_NAME, "requestVote")
        )
        .setType(UNARY)
        .build()!!

//@Serializer(forClass = AppendEntriesResponse::class)
//object AppendEntriesResponseSerializer
//
//@Serializer(forClass = RequestVoteRequest::class)
//object RequestVoteRequestSerializer
//
//@Serializer(forClass = RequestVoteResponse::class)
//object RequestVoteResponseSerializer
