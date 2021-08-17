package it.unitn.disi.ds1.actor;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.message.*;
import it.unitn.disi.ds1.message.welcome.CoordinatorWelcomeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public final class Coordinator extends Actor {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Coordinator.class);

    /**
     * List of available {@link DataStore DataStore(s)}.
     */
    private final List<ActorRef> dataStores;
    private final HashMap<UUID, ActorRef> transactions;
    private final List<Boolean> decisions;

    public Coordinator(int id) {
        super(id);
        this.dataStores = new ArrayList<>();
        this.transactions = new HashMap<>();
        this.decisions = new ArrayList<>();

        LOGGER.debug("Coordinator {} initialized", id);
    }

    public static Props props(int id) {
        return Props.create(Coordinator.class, () -> new Coordinator(id));
    }

    /*-- Actor methods -------------------------------------------------------- */
    private ActorRef serverByKey(int key) {
        return dataStores.get(key / 10);
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
    private void onCoordinatorWelcomeMessage(CoordinatorWelcomeMessage message) {
        LOGGER.debug("Coordinators {} received welcome message: {}", id, message);

        // Data Stores
        dataStores.clear();
        dataStores.addAll(message.dataStores);
    }


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
        this.dataStores.forEach(i -> i.tell(new RequestMsg(msg.transactionId, msg.commit), getSelf()));
        if (!msg.commit) {
            // Reply immediately to Abort client decision
            getSender().tell(new TxnResultMsg(false), getSelf());
        }
    }

    private void onResponseMsg(ResponseMsg msg) {
        // Store Yes or No from servers
        this.decisions.add(msg.decision);
        if (this.decisions.size() == this.dataStores.size()) {
            // Communicate Abort or Commit to servers
            boolean decision = checkCommit();
            System.out.printf("%s decides:%s\n", getSelf().path().name(), decision);
            this.dataStores.forEach(i -> i.tell(new DecisionMsg(msg.transactionId, decision), getSender()));
            this.transactions.get(msg.transactionId).tell(new TxnResultMsg(decision), getSender());
            this.decisions.clear();
            this.transactions.remove(msg.transactionId);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CoordinatorWelcomeMessage.class, this::onCoordinatorWelcomeMessage)
                .build();

        /*return receiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadResultCoordMsg.class, this::onReadResultCoordMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(TxnEndMsg.class, this::onTxnEndMsg)
                .match(ResponseMsg.class, this::onResponseMsg)
                .build();*/
    }
}