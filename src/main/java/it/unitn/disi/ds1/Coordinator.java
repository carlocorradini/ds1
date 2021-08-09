package it.unitn.disi.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.message.TxnAcceptMsg;
import it.unitn.disi.ds1.message.TxnBeginMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public final class Coordinator extends AbstractActor {
    private final ArrayList<Integer> TxnID;
    private final List<ActorRef> servers;
    // Store client ID and transaction ID
    private final HashMap<Integer, UUID> transactionId;

    public Coordinator(List<ActorRef> servers) {
        this.TxnID = new ArrayList<>();
        this.servers = new ArrayList<>(servers);
        this.transactionId = new HashMap<>();
    }

    static public Props props(List<ActorRef> servers) {
        return Props.create(Coordinator.class, () -> new Coordinator(servers));
    }

    /*-- Actor methods -------------------------------------------------------- */

    /*-- Message handlers ----------------------------------------------------- */

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        final UUID transactionID = UUID.randomUUID();
        this.transactionId.put(msg.clientId, transactionID);
        getSender().tell(new TxnAcceptMsg(), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                //.match(ReadMsg.class,  this::onReadMsg)
                //.match(WriteMsg.class,  this::onWriteMsg)
                .build();
    }
}
