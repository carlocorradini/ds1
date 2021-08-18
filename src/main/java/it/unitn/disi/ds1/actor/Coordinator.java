package it.unitn.disi.ds1.actor;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.message.ops.read.ReadResultMessage;
import it.unitn.disi.ds1.message.ops.read.ReadCoordinatorMessage;
import it.unitn.disi.ds1.message.ops.read.ReadMessage;
import it.unitn.disi.ds1.message.ops.read.ReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.ops.write.WriteCoordinatorMessage;
import it.unitn.disi.ds1.message.ops.write.WriteMessage;
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
    // TODO Let's see
    private static class TxnMapping {
        public final int clientId;
        public final ActorRef actorRef;

        public TxnMapping(int clientId, ActorRef actorRef) {
            this.clientId = clientId;
            this.actorRef = actorRef;
        }
    }

    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Coordinator.class);

    /**
     * List of available {@link DataStore DataStore(s)}.
     */
    private final List<ActorRef> dataStores;

    /**
     * Mapping between {@link UUID transactionId} and {@link TxnMapping}.
     */
    private final HashMap<UUID, TxnMapping> transactions;

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
                .match(ReadMessage.class, this::onReadMessage)
                .match(ReadResultCoordinatorMessage.class, this::onReadResultCoordinatorMessage)
                .match(WriteMessage.class, this::onWriteMessage)
                .build();
    }

    // --- Methods ---

    /**
     * Return {@link DataStore} {@link ActorRef} by {@link it.unitn.disi.ds1.Item} key.
     *
     * @param key Item key.
     * @return DataStore actorRef
     */
    private ActorRef dataStoreByItemKey(int key) {
        final ActorRef dataStore = dataStores.get(key / 10);

        if (dataStore == null) {
            LOGGER.error("Unable to find Data Store by Item key {}", key);
            // FIXME Try with something compatible with Akka
            System.exit(1);
        }

        return dataStore;
    }

    /**
     * Return {@link TxnMapping} by {@link UUID transaction id}.
     *
     * @param transactionId Transaction id
     * @return TxnMapping
     */
    private TxnMapping txnMappingByTransactionId(UUID transactionId) {
        final TxnMapping txnMapping = transactions.get(transactionId);

        if (txnMapping == null) {
            LOGGER.error("Unable to find TxnMapping by transactionId {}", transactionId);
            // FIXME Try with something compatible with Akka
            System.exit(1);
        }

        return txnMapping;
    }

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
        final TxnAcceptMessage outMessage = new TxnAcceptMessage(transactionId);
        this.transactions.put(transactionId, new TxnMapping(message.clientId, getSender()));
        getSender().tell(outMessage, getSelf());

        LOGGER.debug("Coordinator {} send TxnAcceptMessage: {}", id, outMessage);
    }

    /**
     * Callback for {@link ReadMessage} message.
     *
     * @param message Received message
     */
    private void onReadMessage(ReadMessage message) {
        LOGGER.debug("Coordinator {} received ReadMessage: {}", id, message);

        final ActorRef dataStore = dataStoreByItemKey(message.key);
        final ReadCoordinatorMessage outMessage = new ReadCoordinatorMessage(message.transactionId, message.key);
        dataStore.tell(outMessage, getSelf());

        LOGGER.debug("Coordinator {} send ReadCoordinatorMessage: {}", id, outMessage);
    }

    /**
     * Callback for {@link ReadResultCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onReadResultCoordinatorMessage(ReadResultCoordinatorMessage message) {
        LOGGER.debug("Coordinator {} received ReadResultCoordinatorMessage: {}", id, message);

        final TxnMapping txnMapping = txnMappingByTransactionId(message.transactionId);
        final ReadResultMessage outMessage = new ReadResultMessage(message.transactionId, message.key, message.value);
        txnMapping.actorRef.tell(outMessage, getSelf());

        LOGGER.debug("Coordinator {} send ReadResultMessage: {}", id, outMessage);
    }

    /**
     * Callback for {@link WriteMessage} message.
     *
     * @param message Received message
     */
    private void onWriteMessage(WriteMessage message) {
        LOGGER.debug("Coordinator {} received WriteMessage: {}", id, message);

        final ActorRef dataStore = dataStoreByItemKey(message.key);
        final WriteCoordinatorMessage outMessage = new WriteCoordinatorMessage(message.transactionId, message.key, message.value);
        dataStore.tell(outMessage, getSelf());

        LOGGER.debug("Coordinator {} send WriteCoordinatorMessage: {}", id, outMessage);
    }

    /*-- Actor methods -------------------------------------------------------- */
    private boolean checkCommit() {
        for (Boolean decision : this.decisions) {
            if (!decision) {
                return false;
            }
        }
        return true;
    }

    /*-- Message handlers ----------------------------------------------------- */
    /*private void onReadResultCoordMsg(ReadResultCoordMsg msg) {
        this.transactions.get(msg.transactionId).tell(new ReadResultMsg(msg.transactionId, msg.key, msg.value), getSelf());
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
    }*/
}
