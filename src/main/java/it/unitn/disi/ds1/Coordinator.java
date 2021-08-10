package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.message.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;

public final class Coordinator extends AbstractActor {
    private final List<ActorRef> servers;
    private final HashMap<UUID, ActorRef> transactions;

    public Coordinator(List<ActorRef> servers) {
        this.servers = new ArrayList<>(servers);
        this.transactions = new HashMap<>();
    }

    static public Props props(List<ActorRef> servers) {
        return Props.create(Coordinator.class, () -> new Coordinator(servers));
    }

    /*-- Actor methods -------------------------------------------------------- */
    private ActorRef serverByKey(int key) {
        return servers.get(key / 10);
    }

    /*-- Message handlers ----------------------------------------------------- */

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        final UUID transactionId = UUID.randomUUID();
        this.transactions.put(transactionId, getSender());
        getSender().tell(new TxnAcceptMsg(transactionId), getSelf());
    }

    private void onReadMsg(ReadMsg msg) {
        serverByKey(msg.key).tell(new ReadCoordMsg(msg.transactionId, msg.key), getSelf());
    }

    private void onReadResultCoordMsg(ReadResultCoordMsg msg) {
        this.transactions.get(msg.transactionId).tell(new ReadResultMsg(msg.transactionId, msg.key, msg.value), getSelf());
    }

    private void onWriteMsg(WriteMsg msg) {
        serverByKey(msg.key).tell(new WriteCoordMsg(msg.transactionId, msg.key, msg.value), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadResultCoordMsg.class, this::onReadResultCoordMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .build();
    }
}
