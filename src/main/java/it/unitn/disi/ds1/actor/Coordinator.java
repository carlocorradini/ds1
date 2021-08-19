package it.unitn.disi.ds1.actor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Pair;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecisionMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcResponseMessage;
import it.unitn.disi.ds1.message.txn.TxnEndMessage;
import it.unitn.disi.ds1.message.txn.TxnResultMessage;
import it.unitn.disi.ds1.message.op.read.ReadResultMessage;
import it.unitn.disi.ds1.message.op.read.ReadCoordinatorMessage;
import it.unitn.disi.ds1.message.op.read.ReadMessage;
import it.unitn.disi.ds1.message.op.read.ReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.op.write.WriteCoordinatorMessage;
import it.unitn.disi.ds1.message.op.write.WriteMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcRequestMessage;
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

    /**
     * Mapping from {@link UUID transaction Id} to {@link ActorRef client ref}.
     */
    private final Map<UUID, ActorRef> transactionIdToClientRef;

    /**
     * Mapping from {@link Integer client id} to {@link UUID transaction id}.
     */
    private final Map<Integer, UUID> clientIdToTransactionId;

    // TODO
    private final Map<UUID, Set<Integer>> dataStoresAffectedInTransaction;

    // TODO
    private final Map<UUID, List<Pair<Integer, Boolean>>> TransactionDecisions;

    // --- Constructors ---

    /**
     * Construct a new Coordinator class.
     *
     * @param id Coordinator id
     */
    public Coordinator(int id) {
        super(id);
        this.dataStores = new ArrayList<>();
        this.transactionIdToClientRef = new HashMap<>();
        this.clientIdToTransactionId = new HashMap<>();
        this.dataStoresAffectedInTransaction = new HashMap<>();
        this.TransactionDecisions = new HashMap<>();

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
                .match(TxnEndMessage.class, this::onTxnEndMessage)
                .match(TwoPcResponseMessage.class, this::onResponseMessage)
                .build();
    }

    // --- Methods ---

    /**
     * Return {@link DataStore} id by {@link it.unitn.disi.ds1.Item} key.
     *
     * @param key Item key
     * @return DataStore id
     */
    private int dataStoreIdByItemKey(int key) {
        return key / 10;
    }

    /**
     * Return {@link DataStore} {@link ActorRef} by {@link it.unitn.disi.ds1.Item} key.
     *
     * @param key Item key
     * @return DataStore actorRef
     */
    private ActorRef dataStoreRefByItemKey(int key) {
        final ActorRef dataStore = dataStores.get(dataStoreIdByItemKey(key));

        if (dataStore == null) {
            LOGGER.error("Coordinator {} is unable to find Data Store by Item key {}", id, key);
            getContext().system().terminate();
        }

        return dataStore;
    }

    /**
     * Return true if all {@link DataStore} have decided to commit, otherwise false.
     *
     * @param decisions List of DataStores decisions
     * @return True can commit, otherwise not
     */
    private boolean canCommit(List<Pair<Integer, Boolean>> decisions) {
        return decisions.stream().allMatch(Pair::second);
    }

    // --- Message handlers --

    /**
     * Callback for {@link CoordinatorWelcomeMessage} message.
     *
     * @param message Received message
     */
    private void onCoordinatorWelcomeMessage(CoordinatorWelcomeMessage message) {
        LOGGER.debug("Coordinator {} received CoordinatorWelcomeMessage: {}", id, message);

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
        final TxnAcceptMessage outMessage = new TxnAcceptMessage();
        clientIdToTransactionId.put(message.clientId, transactionId);
        transactionIdToClientRef.put(transactionId, getSender());

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

        final UUID transactionId = clientIdToTransactionId.get(message.clientId);
        final ActorRef dataStore = dataStoreRefByItemKey(message.key);
        final ReadCoordinatorMessage outMessage = new ReadCoordinatorMessage(transactionId, message.key);
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

        final ActorRef clientRef = transactionIdToClientRef.get(message.transactionId);
        final ReadResultMessage outMessage = new ReadResultMessage(message.key, message.value);
        clientRef.tell(outMessage, getSelf());

        LOGGER.debug("Coordinator {} send ReadResultMessage: {}", id, outMessage);
    }

    /**
     * Callback for {@link WriteMessage} message.
     *
     * @param message Received message
     */
    private void onWriteMessage(WriteMessage message) {
        LOGGER.debug("Coordinator {} received WriteMessage: {}", id, message);

        // Forward write request to DataStore
        final UUID transactionId = clientIdToTransactionId.get(message.clientId);
        final ActorRef dataStore = dataStoreRefByItemKey(message.key);
        final WriteCoordinatorMessage outMessage = new WriteCoordinatorMessage(transactionId, message.key, message.value);
        dataStore.tell(outMessage, getSelf());

        // Add Data Store affected in transaction
        final Set<Integer> itemsAffected = dataStoresAffectedInTransaction.computeIfAbsent(transactionId, k -> new HashSet<>());
        final int dataStoreId = dataStoreIdByItemKey(message.key);
        itemsAffected.add(dataStoreId);
        LOGGER.trace("Coordinator {} add DataStore {} to the affected DataStore(s) in transaction {}", id, dataStoreId, transactionId);

        LOGGER.debug("Coordinator {} send WriteCoordinatorMessage: {}", id, outMessage);
    }

    /**
     * Callback for {@link TxnEndMessage} message.
     *
     * @param message Received message
     */
    private void onTxnEndMessage(TxnEndMessage message) {
        LOGGER.debug("Coordinator {} received TxnEndMessage {}", id, message);

        final UUID transactionId = clientIdToTransactionId.get(message.clientId);
        final TwoPcRequestMessage outMessage = new TwoPcRequestMessage(transactionId, message.commit);

        // FIXME Cambiare id di accesso Data Store ???
        final Set<ActorRef> dataStoresToContact = new HashSet<>();
        dataStoresAffectedInTransaction.get(transactionId).forEach(dataStoreId -> dataStoresToContact.add(dataStores.get(dataStoreId)));
        dataStoresToContact.forEach(i -> i.tell(outMessage, getSelf()));

        LOGGER.debug("Coordinator {} send RequestMessage: {}", id, outMessage);

        if (!message.commit) {
            // Reply immediately to Abort client decision
            final TxnResultMessage abortMessage = new TxnResultMessage(false);
            getSender().tell(abortMessage, getSelf());
            LOGGER.debug("Coordinator {} send TxnResultMessage {}", id, abortMessage);
        }
    }

    /**
     * Callback for {@link TwoPcResponseMessage} message.
     *
     * @param message Received message
     */
    private void onResponseMessage(TwoPcResponseMessage message) {
        LOGGER.debug("Coordinator {} received ResponseMessage {}", id, message);

        // Obtain or create the list of DataStores decision
        final List<Pair<Integer, Boolean>> decisions = TransactionDecisions.computeIfAbsent(message.transactionId, k -> new ArrayList<>());
        // Add decision of the DataStore
        decisions.add(Pair.create(message.dataStoreId, message.decision));

        // Data stores affected in current transaction
        final Set<ActorRef> dataStoresToContact = new HashSet<>();
        dataStoresAffectedInTransaction.get(message.transactionId).forEach(dataStoreId -> dataStoresToContact.add(dataStores.get(dataStoreId)));
        ;

        // Check if the number of decisions has reached all the DataStores
        if (decisions.size() == dataStoresToContact.size()) {
            final boolean canCommit = canCommit(decisions);

            if (canCommit) {
                LOGGER.info("Coordinator {} decided to commit: {}", id, decisions);
            } else {
                LOGGER.info("Coordinator {} decided to abort: {}", id, decisions);
            }

            // Communicate commit decision to DataStores
            final TwoPcDecisionMessage outMessageToDataStore = new TwoPcDecisionMessage(message.transactionId, canCommit);
            dataStoresToContact.forEach(dataStore -> dataStore.tell(outMessageToDataStore, getSender()));
            LOGGER.debug("Coordinator {} send to DataStores DecisionMessage: {}", id, outMessageToDataStore);

            // Communicate commit decision to Client
            final TxnResultMessage outMessageToClient = new TxnResultMessage(canCommit);
            transactionIdToClientRef.get(message.transactionId).tell(outMessageToClient, getSender());
            LOGGER.debug("Coordinator {} send to Client TxnResultMessage: {}", id, outMessageToClient);

            // Clean
            transactionIdToClientRef.remove(message.transactionId);
            clientIdToTransactionId.values().remove(message.transactionId);
            dataStoresAffectedInTransaction.remove(message.transactionId);
            TransactionDecisions.remove(message.transactionId);
            LOGGER.trace("Coordinator {} clean resources", id);
        }
    }
}
