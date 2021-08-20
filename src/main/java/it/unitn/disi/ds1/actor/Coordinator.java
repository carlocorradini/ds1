package it.unitn.disi.ds1.actor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Pair;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
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
     * {@link DataStore DataStore(s)} metadata.
     */
    private final List<ActorMetadata> dataStores;

    /**
     * Mapping from {@link UUID transaction Id} to {@link ActorRef client ref}.
     */
    private final Map<UUID, ActorRef> transactionIdToClientRef;

    /**
     * Mapping from {@link Integer client id} to {@link UUID transaction id}.
     */
    private final Map<Integer, UUID> clientIdToTransactionId;

    // TODO
    private final Map<UUID, Set<ActorMetadata>> dataStoresAffectedInTransaction;

    // TODO
    private final Map<UUID, List<Pair<Integer, TwoPcDecision>>> transactionDecisions;

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
        this.transactionDecisions = new HashMap<>();

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
                .match(TwoPcResponseMessage.class, this::onTwoPcResponseMessage)
                .build();
    }

    // --- Methods ---

    /**
     * Return {@link DataStore} {@link ActorRef} by {@link Item} key.
     *
     * @param key Item key
     * @return DataStore actorRef
     */
    private ActorMetadata dataStoreMetadataByItemKey(int key) {
        final ActorMetadata dataStore = dataStores.get(key / 10);

        if (dataStore == null) {
            LOGGER.error("Coordinator {} is unable to find Data Store by Item key {}", id, key);
            getContext().system().terminate();
        }

        return dataStore;
    }

    /**
     * Return true if all {@link DataStore} in decisions have decided to commit, otherwise false.
     *
     * @param decisions List of DataStores decisions
     * @return True can commit, otherwise not
     */
    private boolean canCommit(List<Pair<Integer, TwoPcDecision>> decisions) {
        return decisions.stream().allMatch(decision -> decision.second() == TwoPcDecision.COMMIT);
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
        LOGGER.debug("Coordinator {} received from Client {} TxnBeginMessage: {}", id, message.clientId, message);

        // Generate a transaction id and store all relevant data
        final UUID transactionId = UUID.randomUUID();
        clientIdToTransactionId.put(message.clientId, transactionId);
        transactionIdToClientRef.put(transactionId, getSender());

        // Inform Client that the transaction has been accepted
        final TxnAcceptMessage outMessage = new TxnAcceptMessage();
        getSender().tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to Client {} TxnAcceptMessage: {}", id, message.clientId, outMessage);
    }

    /**
     * Callback for {@link ReadMessage} message.
     *
     * @param message Received message
     */
    private void onReadMessage(ReadMessage message) {
        LOGGER.debug("Coordinator {} received from Client {} ReadMessage: {}", id, message.clientId, message);

        // Obtain correct DataStore
        final ActorMetadata dataStore = dataStoreMetadataByItemKey(message.key);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.clientId);

        // Send to DataStore Item read message
        final ReadCoordinatorMessage outMessage = new ReadCoordinatorMessage(transactionId, message.key);
        dataStore.ref.tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to DataStore {} ReadCoordinatorMessage: {}", id, dataStore.id, outMessage);
    }

    /**
     * Callback for {@link ReadResultCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onReadResultCoordinatorMessage(ReadResultCoordinatorMessage message) {
        LOGGER.debug("Coordinator {} received ReadResultCoordinatorMessage: {}", id, message);

        // Obtain Client ref
        final ActorRef clientRef = transactionIdToClientRef.get(message.transactionId);

        // Send to Client Item read reply message
        final ReadResultMessage outMessage = new ReadResultMessage(message.key, message.value);
        clientRef.tell(outMessage, getSelf());
        // TODO Client id
        LOGGER.debug("Coordinator {} send ReadResultMessage: {}", id, outMessage);
    }

    /**
     * Callback for {@link WriteMessage} message.
     *
     * @param message Received message
     */
    private void onWriteMessage(WriteMessage message) {
        LOGGER.debug("Coordinator {} received WriteMessage: {}", id, message);

        // Obtain correct DataStore
        final ActorMetadata dataStore = dataStoreMetadataByItemKey(message.key);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.clientId);

        // Add DataStore to the affected for the current transaction
        final Set<ActorMetadata> dataStoresAffected = dataStoresAffectedInTransaction.computeIfAbsent(transactionId, k -> new HashSet<>());
        if (dataStoresAffected.isEmpty()) {
            dataStoresAffected.add(dataStore);
            LOGGER.trace("Coordinator {} add DataStore {} to affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        } else {
            LOGGER.trace("Coordinator {} DataStore {} already present in affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        }

        // Send to DataStore Item write message
        final WriteCoordinatorMessage outMessage = new WriteCoordinatorMessage(transactionId, message.key, message.value);
        dataStore.ref.tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to DataStore {} WriteCoordinatorMessage: {}", id, dataStore.id, outMessage);
    }

    /**
     * Callback for {@link TxnEndMessage} message.
     *
     * @param message Received message
     */
    private void onTxnEndMessage(TxnEndMessage message) {
        LOGGER.debug("Coordinator {} received TxnEndMessage {}", id, message);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.clientId);

        // Send to affected DataStore(s) 2PC request message
        final TwoPcRequestMessage outMessage = new TwoPcRequestMessage(transactionId, message.commit ? TwoPcDecision.COMMIT : TwoPcDecision.ABORT);
        final Set<ActorMetadata> affectedDataStores = dataStoresAffectedInTransaction.get(transactionId);
        affectedDataStores.forEach(dataStore -> {
            dataStore.ref.tell(outMessage, getSelf());
            LOGGER.trace("Coordinator {} send to affected DataStore {} involving transaction {} TwoPcRequestMessage: {}", id, dataStore.id, transactionId, outMessage);
        });
        LOGGER.debug("Coordinator {} send to {} affected DataStore(s) TwoPcRequestMessage: {}", id, affectedDataStores.size(), outMessage);

        // If Client decided to abort, reply immediately
        if (!message.commit) {
            final TxnResultMessage abortMessage = new TxnResultMessage(false);
            getSender().tell(abortMessage, getSelf());
            LOGGER.debug("Coordinator {} send to Client {} TxnResultMessage: {}", id, message.clientId, abortMessage);
        }
    }

    /**
     * Callback for {@link TwoPcResponseMessage} message.
     *
     * @param message Received message
     */
    private void onTwoPcResponseMessage(TwoPcResponseMessage message) {
        LOGGER.debug("Coordinator {} received from DataStore {} TwoPcResponseMessage: {}", id, message.dataStoreId, message);

        // Obtain or create DataStore(s) decisions
        final List<Pair<Integer, TwoPcDecision>> decisions = transactionDecisions.computeIfAbsent(message.transactionId, k -> new ArrayList<>());
        // Add decision of the DataStore
        decisions.add(Pair.create(message.dataStoreId, message.decision));

        // Data stores affected in current transaction
        final Set<ActorMetadata> affectedDataStores = dataStoresAffectedInTransaction.get(message.transactionId);

        // Check if the number of decisions has reached all affected DataStore(s)
        if (decisions.size() == affectedDataStores.size()) {
            final TwoPcDecision decision;

            if (canCommit(decisions)) {
                decision = TwoPcDecision.COMMIT;
                LOGGER.info("Coordinator {} decided to commit transaction {}: {}", id, message.transactionId, decisions);
            } else {
                decision = TwoPcDecision.ABORT;
                LOGGER.info("Coordinator {} decided to abort transaction {}: {}", id, message.transactionId, decisions);
            }

            // Communicate commit decision to all affected DataStore(s)
            final TwoPcDecisionMessage outMessageToDataStore = new TwoPcDecisionMessage(message.transactionId, decision);
            affectedDataStores.forEach(dataStore -> {
                dataStore.ref.tell(outMessageToDataStore, getSender());
                LOGGER.trace("Coordinator {} send to affected DataStore {} involving transaction {} TwoPcDecisionMessage: {}", id, dataStore.id, message.transactionId, outMessageToDataStore);
            });
            LOGGER.debug("Coordinator {} send to {} affected DataStore(s) TwoPcDecisionMessage: {}", id, affectedDataStores.size(), outMessageToDataStore);

            // Communicate commit decision to Client
            final TxnResultMessage outMessageToClient = new TxnResultMessage(decision == TwoPcDecision.COMMIT);
            transactionIdToClientRef.get(message.transactionId).tell(outMessageToClient, getSender());
            // TODO Client id
            LOGGER.debug("Coordinator {} send to Client TxnResultMessage: {}", id, outMessageToClient);

            // Clean resources
            transactionIdToClientRef.remove(message.transactionId);
            clientIdToTransactionId.values().remove(message.transactionId);
            dataStoresAffectedInTransaction.remove(message.transactionId);
            transactionDecisions.remove(message.transactionId);
            LOGGER.trace("Coordinator {} clean resources involving transaction {}", id, message.transactionId);
        }
    }
}
