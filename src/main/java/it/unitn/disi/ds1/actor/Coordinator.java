package it.unitn.disi.ds1.actor;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.message.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public final class Coordinator extends Actor {
    private final List<ActorRef> servers;
    private final HashMap<UUID, ActorRef> transactions;
    private final List<Boolean> decisions;

    public Coordinator(int id, List<ActorRef> servers) {
        super(id);
        this.servers = new ArrayList<>(servers);
        this.transactions = new HashMap<>();
        this.decisions = new ArrayList<>();
    }

    public static Props props(int id, List<ActorRef> servers) {
        return Props.create(Coordinator.class, () -> new Coordinator(id, servers));
    }

    /*-- Actor methods -------------------------------------------------------- */
    private ActorRef serverByKey(int key) {
        return servers.get(key / 10);
    }

    private boolean checkCommit() {
        for (Boolean decision : this.decisions) {
            if (!decision) {
                return false;
            }
        }
        return true;
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

    private void onTxnEndMsg(TxnEndMsg msg) {
        System.out.printf("Start 2PC from %s for transaction:%s with decision by %s:%s\n", getSelf().path().name(), msg.transactionId, getSender().path().name(), msg.commit);
        this.servers.forEach(i -> i.tell(new RequestMsg(msg.transactionId, msg.commit), getSelf()));
        if (!msg.commit) {
            // Reply immediately to Abort client decision
            getSender().tell(new TxnResultMsg(false), getSelf());
        }
    }

    private void onResponseMsg(ResponseMsg msg) {
        // Store Yes or No from servers
        this.decisions.add(msg.decision);
        if (this.decisions.size() == this.servers.size()) {
            // Communicate Abort or Commit to servers
            boolean decision = checkCommit();
            System.out.printf("%s decides:%s\n", getSelf().path().name(), decision);
            this.servers.forEach(i -> i.tell(new DecisionMsg(msg.transactionId, decision), getSender()));
            this.transactions.get(msg.transactionId).tell(new TxnResultMsg(decision), getSender());
            this.decisions.clear();
            this.transactions.remove(msg.transactionId);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadResultCoordMsg.class, this::onReadResultCoordMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(TxnEndMsg.class, this::onTxnEndMsg)
                .match(ResponseMsg.class, this::onResponseMsg)
                .build();
    }
}
