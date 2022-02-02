package org.gertje.vlot.kvstr

import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import kotlinx.serialization.builtins.PairSerializer
import kotlinx.serialization.builtins.serializer
import org.gertje.vlot.ConsensusModule
import org.gertje.vlot.PeerId
import org.gertje.vlot.grpc.GrpcRaftClient
import org.gertje.vlot.grpc.GrpcRaftServer
import org.gertje.vlot.state.State

class Node(
    id: PeerId,
    port: Int,
    peerUrls: Map<PeerId, Pair<String, Int>>,
) {
    private val serializer = PairSerializer(String.serializer(), Int.serializer())
    private val heartBeatTicker = ChannelTicker(300, true)
    private val electionTicker = ChannelTicker(1000)
    private val heartBeatTickerChannel: ReceiveChannel<Unit> = heartBeatTicker.tickChannel
    private val electionTickerChannel: ReceiveChannel<Unit> = electionTicker.tickChannel

    private val keyValueStore = KeyValueStore()

    private val log = SimpleLog<Pair<String, Int>>()
    private val state = State(peerUrls.size + 1, log)
    private val consensusModule = ConsensusModule(
        id,
        peerUrls.keys + id,
        GrpcRaftClient(peerUrls, serializer),
        heartBeatTicker,
        electionTicker,
        state,
        NoStateWriter(),
        keyValueStore
    )

    private val server: Server = ServerBuilder
        .forPort(port)
        .addService(GrpcRaftServer(consensusModule, serializer))
        .build()

    fun start(): Unit = runBlocking {

        launch {
            while (true) {
                println("Next loop")
                select<Unit> {
                    electionTickerChannel.onReceive {
                        println("Received election tick")
                        launch {
                            consensusModule.onElectionTimeout()
                        }
                    }
                    heartBeatTickerChannel.onReceive {
                        println("Received heartbeat tick")
                        launch {
                            consensusModule.onHeartbeatTimeout()
                        }
                    }
                }
            }
        }

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() = println(keyValueStore["wim"])
        })

        server.start()
    }
}
