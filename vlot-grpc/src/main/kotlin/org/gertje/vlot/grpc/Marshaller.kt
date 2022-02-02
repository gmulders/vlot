package org.gertje.vlot.grpc

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import io.grpc.MethodDescriptor
import kotlinx.serialization.KSerializer
import mu.KotlinLogging
import org.gertje.vlot.AppendEntriesRequest
import org.gertje.vlot.Entry
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream

private val log = KotlinLogging.logger {}

internal fun <S> marshallerFor(kSerializer: KSerializer<S>) = object : MethodDescriptor.Marshaller<S> {
    override fun stream(value: S): InputStream =
        ByteArrayInputStream(MsgPack.Default.encodeToByteArray(kSerializer, value))

    override fun parse(stream: InputStream): S =
        MsgPack.Default.decodeFromByteArray(kSerializer, stream.readAllBytes())
}

internal fun <S> appendEntriesRequestMarshallerFor(kSerializer: KSerializer<S>) = object : MethodDescriptor.Marshaller<AppendEntriesRequest<S>> {
    override fun stream(value: AppendEntriesRequest<S>): InputStream {
        val byteArrayOutputStream = ByteArrayOutputStream()
        val dataOutputStream = DataOutputStream(byteArrayOutputStream)

        assert(value.term > 0)
        assert(value.leaderId > 0)
        assert(value.prevLogIndex > 0)
        assert(value.prevLogTerm > 0)
        assert(value.leaderCommit > 0)

        dataOutputStream.writeRawVarInt(value.term)
        dataOutputStream.writeRawVarInt(value.leaderId.toLong())
        dataOutputStream.writeRawVarInt(value.prevLogIndex + 1)
        dataOutputStream.writeRawVarInt(value.prevLogTerm + 1)
        dataOutputStream.writeRawVarInt(value.entries.size.toLong())
        value.entries.forEach { entry ->
            assert(entry.term > 0)
            dataOutputStream.writeRawVarInt(entry.term)
            val bytes = MsgPack.Default.encodeToByteArray(kSerializer, entry.value)
            dataOutputStream.writeRawVarInt(bytes.size.toLong())
            dataOutputStream.write(bytes)
        }
        dataOutputStream.writeRawVarInt(value.leaderCommit + 1)
        log.info { "Message length: ${byteArrayOutputStream.toByteArray().size}" }
        return ByteArrayInputStream(byteArrayOutputStream.toByteArray())
    }

    override fun parse(stream: InputStream): AppendEntriesRequest<S> {
        val dataInputStream = DataInputStream(stream)

        return AppendEntriesRequest(
            term = dataInputStream.readRawVarInt(),
            leaderId = dataInputStream.readRawVarInt().toInt(),
            prevLogIndex = dataInputStream.readRawVarInt() - 1,
            prevLogTerm = dataInputStream.readRawVarInt() - 1,
            (0 until dataInputStream.readRawVarInt().toInt()).map {
                Entry(
                    term = dataInputStream.readRawVarInt(),
                    value = MsgPack.Default.decodeFromByteArray(kSerializer, stream.readNBytes(dataInputStream.readRawVarInt().toInt()))
                )
            },
            leaderCommit = dataInputStream.readRawVarInt() - 1
        )
    }
}