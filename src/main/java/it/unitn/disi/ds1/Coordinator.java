package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.message.TxnAcceptMsg;
import it.unitn.disi.ds1.message.TxnBeginMsg;
import it.unitn.disi.ds1.message.ReadMsg;
import it.unitn.disi.ds1.message.ReadResultMsg;
import it.unitn.disi.ds1.message.ReadCoordMsg;
import it.unitn.disi.ds1.message.ReadResultCoordMsg;
import it.unitn.disi.ds1.message.WriteMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;

public final class Coordinator extends AbstractActor {
    private final List<ActorRef> servers;
    // Store client ID and transaction ID
    private final HashMap<ActorRef, UUID> clientTxnID;

    public Coordinator(List<ActorRef> servers) {
        this.servers = new ArrayList<>(servers);
        this.clientTxnID = new HashMap<>();
    }

    static public Props props(List<ActorRef> servers) {
        return Props.create(Coordinator.class, () -> new Coordinator(servers));
    }

    /*-- Actor methods -------------------------------------------------------- */

    /*-- Message handlers ----------------------------------------------------- */

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        final UUID transactionId = UUID.randomUUID();
        this.clientTxnID.put(getSender(), transactionId);
        getSender().tell(new TxnAcceptMsg(), getSelf());
    }

    private void onReadMsg(ReadMsg msg) {
        int index = (int)msg.key / 10;
        this.servers[index].tell(new ReadCoordMsg(this.clientTxnID.get(getSender()), msg.key), getSelf());
    }

    // Come forwardare la risposta del server al client che ha fatto la richiesta di READ?
    private void onReadResultCoordMsg(ReadResultCoordMsg msg) {
        for (Map.Entry me : this.clientTxnID.entrySet()) {
            if (me.getValue().equals(msg.transactionId)) {
                me.getKey().tell(new ReadResultMsg(msg.key, msg.value), getSelf());
                break;
            }
        }
    }

    private void onWriteMsg(WriteMsg msg) {
        int index = (int)msg.key / 10;
        this.servers[index].tell(new WriteCoordMsg(this.transactionId.get(msg.clientId), msg.key, msg.value), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ReadMsg.class,  this::onReadMsg)
                .match(ReadResultCoordMsg.class,  this::onReadResultCoordMsg)
                .match(WriteMsg.class,  this::onWriteMsg)
                .build();
    }
}
