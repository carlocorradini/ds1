package it.unitn.ds1;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;

import it.unitn.ds1.Message.*;

public class Coordinator extends AbstractActor {
    private ArrayList<Integer> TxnID = new ArrayList<Integer>();
    private List<ActorRef> servers = new ArrayList<>();

    // Store client ID and transaction ID
    private HashMap<Integer, String> transactionId = new HashMap<Integer, String>();

    /*-- Actor constructor ---------------------------------------------------- */

    public Coordinator(List<ActorRef> servers) {
        this.servers.addAll(servers);
    }

    static public Props props(List<ActorRef> servers) {
        return Props.create(Coordinator.class, () -> new Coordinator(servers));
    }

    /*-- Actor methods -------------------------------------------------------- */

    /*-- Message handlers ----------------------------------------------------- */

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        String uniqueID = UUID.randomUUID().toString();
        this.transactionId.put(msg.clientId, uniqueID);
        getSender().tell(new TxnAcceptMsg(), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
                .match(TxnBeginMsg.class,  this::onTxnBeginMsg)
                //.match(ReadMsg.class,  this::onReadMsg)
                //.match(WriteMsg.class,  this::onWriteMsg)
                .build();
    }
}
