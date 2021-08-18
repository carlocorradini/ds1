package it.unitn.disi.ds1.actor;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.message.*;
import it.unitn.disi.ds1.message.txn.TxnAcceptMessage;
import it.unitn.disi.ds1.message.txn.TxnBeginMessage;
import it.unitn.disi.ds1.message.welcome.CoordinatorWelcomeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Coordinator {@link Actor actor} class.
 */
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

    // --- Constructors ---

    /**
     * Construct a new Coordinator class.
     *
     * @param id Coordinator id
     */
    public Coordinator(int id) {
        super(id);
        this.dataStores = new ArrayList<>();
        this.transactions = new HashMap<>();
        this.decisions = new ArrayList<>();

        LOGGER.debug("Coordinator {} initialized", id);
    }

    /**
     * Return Coordinator {@link Props}
     *
     * @param id Coordinator id
     * @return Coordinator {@link Props}
     */
    public static Props props(int id) {
        return Props.create(Coordinator.class, () -> new Coordinator(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CoordinatorWelcomeMessage.class, this::onCoordinatorWelcomeMessage)
                .match(TxnBeginMessage.class, this::onTxnBeginMessage)
                .build();
    }

    // --- Methods ---

    // --- Message handlers --

    /**
     * Callback for {@link CoordinatorWelcomeMessage} message.
     *
     * @param message Received message
     */
    private void onCoordinatorWelcomeMessage(CoordinatorWelcomeMessage message) {
        LOGGER.debug("Coordinator {} received CoordinatorWelcomeMessage: {}", id, message);

        // Data Stores
        dataStores.clear();
        dataStores.addAll(message.dataStores);
    }

    /**
     * Callback for {@link TxnBeginMessage} message.
     *
     * @param message Received message
     */
    private void onTxnBeginMessage(TxnBeginMessage message) {
        LOGGER.debug("Coordinator {} received TxnBeginMessage: {}", id, message);

        final UUID transactionId = UUID.randomUUID();
        this.transactions.put(transactionId, getSender());
        getSender().tell(new TxnAcceptMessage(transactionId), getSelf());

        LOGGER.info("Coordinator {} send TxnAcceptMessage to client {} with transactionId {}", id, message.clientId, transactionId);
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
}
