package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.DataStoreDecision;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecisionMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcVoteResponseMessage;
import it.unitn.disi.ds1.message.snapshot.SnapshotMessage;
import it.unitn.disi.ds1.message.snapshot.SnapshotResultMessage;
import it.unitn.disi.ds1.message.txn.TxnEndMessage;
import it.unitn.disi.ds1.message.txn.TxnEndResultMessage;
import it.unitn.disi.ds1.message.txn.read.TxnReadResultMessage;
import it.unitn.disi.ds1.message.txn.read.TxnReadCoordinatorMessage;
import it.unitn.disi.ds1.message.txn.read.TxnReadMessage;
import it.unitn.disi.ds1.message.txn.read.TxnReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.txn.write.TxnWriteCoordinatorMessage;
import it.unitn.disi.ds1.message.txn.write.TxnWriteMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcVoteRequestMessage;
import it.unitn.disi.ds1.message.txn.TxnAcceptMessage;
import it.unitn.disi.ds1.message.txn.TxnBeginMessage;
import it.unitn.disi.ds1.message.welcome.CoordinatorWelcomeMessage;
import it.unitn.disi.ds1.util.JsonUtil;
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
     * Mapping from {@link UUID transaction Id} to {@link ActorMetadata client metadata}.
     */
    private final Map<UUID, ActorMetadata> transactionIdToClient;

    /**
     * Mapping from {@link Integer client id} to {@link UUID transaction id}.
     */
    private final Map<Integer, UUID> clientIdToTransactionId;

    /**
     * {@link DataStore DataStore(s)} affected in a {@link UUID transaction}.
     */
    private final Map<UUID, Set<ActorMetadata>> dataStoresAffectedInTransaction;

    /**
     * {@link DataStore DataStore(s)} decisions for a {@link UUID transaction}.
     */
    private final Map<UUID, Set<DataStoreDecision>> transactionDecisions;

    /**
     * {@link DataStore DataStore(s)} storage snapshot(s).
     */
    private final Map<Integer, Item> snapshot;

    // --- Constructors ---

    /**
     * Construct a new Coordinator class.
     *
     * @param id Coordinator id
     */
    public Coordinator(int id) {
        super(id);
        this.dataStores = new ArrayList<>();
        this.transactionIdToClient = new HashMap<>();
        this.clientIdToTransactionId = new HashMap<>();
        this.dataStoresAffectedInTransaction = new HashMap<>();
        this.transactionDecisions = new HashMap<>();
        this.snapshot = new TreeMap<>();

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
                .match(TxnReadMessage.class, this::onTxnReadMessage)
                .match(TxnReadResultCoordinatorMessage.class, this::onTxnReadResultCoordinatorMessage)
                .match(TxnWriteMessage.class, this::onTxnWriteMessage)
                .match(TxnEndMessage.class, this::onTxnEndMessage)
                .match(TwoPcVoteResponseMessage.class, this::onTwoPcVoteResponseMessage)
                .match(SnapshotMessage.class, this::onSnapshotMessage)
                .match(SnapshotResultMessage.class, this::onSnapshotResultMessage)
                .build();
    }

    // --- Methods ---

    /**
     * Return {@link DataStore} {@link ActorMetadata} by {@link Item} key.
     *
     * @param key Item key
     * @return DataStore actorRef
     */
    private ActorMetadata dataStoreByItemKey(int key) {
        final int dataStoreId = key / 10;
        return dataStores.stream()
                .filter(dataStore -> dataStore.id == dataStoreId)
                .findFirst()
                .orElseThrow(() -> new NullPointerException(String.format("Coordinator %d is unable to obtain DataStore %d via Item key %d", id, dataStoreId, key)));
    }

    /**
     * Return true if all {@link DataStore} in decisions have decided to commit, otherwise false.
     *
     * @param decisions DataStores decisions
     * @return True can commit, otherwise not
     */
    private boolean canCommit(Set<DataStoreDecision> decisions) {
        return decisions.stream()
                .allMatch(decision -> decision.decision == TwoPcDecision.COMMIT);
    }

    /**
     * Terminate the {@link UUID transaction} with the chosen decision.
     *
     * @param transactionId {@link UUID Transaction} id
     * @param decision      Final {@link TwoPcDecision decision}
     */
    private void terminateTransaction(UUID transactionId, TwoPcDecision decision) {
        // Data stores affected in current transaction
        final Set<ActorMetadata> affectedDataStores = dataStoresAffectedInTransaction.getOrDefault(transactionId, new HashSet<>());

        // Communicate decision to all affected DataStore(s), if any
        final TwoPcDecisionMessage outMessageToDataStore = new TwoPcDecisionMessage(id, transactionId, decision);
        affectedDataStores.forEach(dataStore -> {
            dataStore.ref.tell(outMessageToDataStore, getSender());
            LOGGER.trace("Coordinator {} send to affected DataStore {} to {} transaction {} TwoPcDecisionMessage: {}", id, dataStore.id, decision, transactionId, outMessageToDataStore);
        });
        LOGGER.debug("Coordinator {} send to {} affected DataStore(s) to {} transaction {} TwoPcDecisionMessage: {}", id, affectedDataStores.size(), decision, transactionId, outMessageToDataStore);

        // Communicate commit decision to Client
        final ActorMetadata client = transactionIdToClient.get(transactionId);
        final TxnEndResultMessage outMessageToClient = new TxnEndResultMessage(id, decision);
        client.ref.tell(outMessageToClient, getSender());
        LOGGER.debug("Coordinator {} send to Client {} that transaction {} is {} TxnEndResultMessage: {}", id, client.id, transactionId, decision, outMessageToClient);

        // Clean resources
        cleanResources(transactionId);
    }

    /**
     * Clean all resources that involves {@link UUID transaction}.
     *
     * @param transactionId Transaction id
     */
    private void cleanResources(UUID transactionId) {
        if (transactionId == null) return;

        transactionIdToClient.remove(transactionId);
        clientIdToTransactionId.values().remove(transactionId);
        dataStoresAffectedInTransaction.remove(transactionId);
        transactionDecisions.remove(transactionId);
        LOGGER.trace("Coordinator {} clean resources involving transaction {}", id, transactionId);
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
        LOGGER.debug("Coordinator {} received from Client {} TxnBeginMessage: {}", id, message.senderId, message);

        // Check if Client has already a transaction running
        final UUID transactionIdRunning = clientIdToTransactionId.get(message.senderId);
        if (transactionIdRunning != null)
            throw new IllegalStateException(String.format("Coordinator %d received a TxnBeginMessage from Client %d when transaction %s is running", id, message.senderId, transactionIdRunning));

        // Generate a transaction id and store all relevant data
        final UUID transactionId = UUID.randomUUID();
        clientIdToTransactionId.put(message.senderId, transactionId);
        transactionIdToClient.put(transactionId, ActorMetadata.of(message.senderId, getSender()));

        // Inform Client that the transaction has been accepted
        final TxnAcceptMessage outMessage = new TxnAcceptMessage(id);
        getSender().tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to Client {} involving transaction transactionId {} TxnAcceptMessage: {}", id, message.senderId, transactionId, outMessage);
    }

    /**
     * Callback for {@link TxnReadMessage} message.
     *
     * @param message Received message
     */
    private void onTxnReadMessage(TxnReadMessage message) {
        LOGGER.debug("Coordinator {} received from Client {} TxnReadMessage: {}", id, message.senderId, message);

        // Obtain correct DataStore
        final ActorMetadata dataStore = dataStoreByItemKey(message.key);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.senderId);

        // Add DataStore to affected in transaction
        final Set<ActorMetadata> dataStoresAffected = dataStoresAffectedInTransaction.computeIfAbsent(transactionId, k -> new HashSet<>());
        if (dataStoresAffected.add(dataStore)) {
            LOGGER.trace("Coordinator {} add DataStore {} to affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        } else {
            LOGGER.trace("Coordinator {} DataStore {} already present in affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        }

        // Send to DataStore Item read message
        final TxnReadCoordinatorMessage outMessage = new TxnReadCoordinatorMessage(id, transactionId, message.key);
        dataStore.ref.tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to DataStore {} TxnReadCoordinatorMessage: {}", id, dataStore.id, outMessage);
    }

    /**
     * Callback for {@link TxnReadResultCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onTxnReadResultCoordinatorMessage(TxnReadResultCoordinatorMessage message) {
        LOGGER.debug("Coordinator {} received from DataStore {} TxnReadResultCoordinatorMessage: {}", id, message.senderId, message);

        // Obtain Client
        final ActorMetadata client = transactionIdToClient.get(message.transactionId);

        // Send to Client Item read reply message
        final TxnReadResultMessage outMessage = new TxnReadResultMessage(id, message.key, message.value);
        client.ref.tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to Client {} TxnReadResultMessage: {}", id, client.id, outMessage);
    }

    /**
     * Callback for {@link TxnWriteMessage} message.
     *
     * @param message Received message
     */
    private void onTxnWriteMessage(TxnWriteMessage message) {
        LOGGER.debug("Coordinator {} received from Client {} TxnWriteMessage: {}", id, message.senderId, message);

        // Obtain correct DataStore
        final ActorMetadata dataStore = dataStoreByItemKey(message.key);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.senderId);

        // Add DataStore to affected in transaction
        final Set<ActorMetadata> dataStoresAffected = dataStoresAffectedInTransaction.computeIfAbsent(transactionId, k -> new HashSet<>());
        if (dataStoresAffected.add(dataStore)) {
            LOGGER.trace("Coordinator {} add DataStore {} to affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        } else {
            LOGGER.trace("Coordinator {} DataStore {} already present in affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        }

        // Send to DataStore Item write message
        final TxnWriteCoordinatorMessage outMessage = new TxnWriteCoordinatorMessage(id, transactionId, message.key, message.value);
        dataStore.ref.tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to DataStore {} TxnWriteCoordinatorMessage: {}", id, dataStore.id, outMessage);
    }

    /**
     * Callback for {@link TxnEndMessage} message.
     *
     * @param message Received message
     */
    private void onTxnEndMessage(TxnEndMessage message) {
        LOGGER.debug("Coordinator {} received from Client {} TxnEndMessage {}", id, message.senderId, message);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.senderId);

        // Check Client decision
        switch (message.decision) {
            case COMMIT: {
                // Client decided to commit
                LOGGER.info("Coordinator {} informed that Client {} want to COMMIT transaction {}", id, message.senderId, transactionId);
                // Obtain affected DataStore(s) in transaction
                final Set<ActorMetadata> affectedDataStores = dataStoresAffectedInTransaction.getOrDefault(transactionId, new HashSet<>());
                // Check if there is at least one affected DataStore
                if (!affectedDataStores.isEmpty()) {
                    // DataStore(s) affected
                    final TwoPcVoteRequestMessage outMessage = new TwoPcVoteRequestMessage(id, transactionId, TwoPcDecision.COMMIT);
                    affectedDataStores.forEach(dataStore -> {
                        dataStore.ref.tell(outMessage, getSelf());
                        LOGGER.trace("Coordinator {} send to affected DataStore {} if can COMMIT transaction {} TwoPcVoteRequestMessage: {}", id, dataStore.id, transactionId, outMessage);
                    });
                    LOGGER.debug("Coordinator {} send to {} affected DataStore(s) if can COMMIT transaction {} TwoPcVoteRequestMessage: {}", id, affectedDataStores.size(), transactionId, outMessage);
                } else {
                    // No DataStore(s) affected
                    LOGGER.warn("Coordinator {} no DataStore(s) are affected in transaction {}", id, transactionId);
                    terminateTransaction(transactionId, TwoPcDecision.COMMIT);
                }
                break;
            }
            case ABORT: {
                // Client decided to abort
                LOGGER.info("Coordinator {} informed that Client {} want to ABORT transaction {}", id, message.senderId, transactionId);
                terminateTransaction(transactionId, TwoPcDecision.ABORT);
                break;
            }
        }
    }

    /**
     * Callback for {@link TwoPcVoteResponseMessage} message.
     *
     * @param message Received message
     */
    private void onTwoPcVoteResponseMessage(TwoPcVoteResponseMessage message) {
        LOGGER.debug("Coordinator {} received from DataStore {} TwoPcVoteResponseMessage: {}", id, message.senderId, message);

        // Obtain or create DataStore(s) decisions
        final Set<DataStoreDecision> decisions = transactionDecisions.computeIfAbsent(message.transactionId, k -> new HashSet<>());
        // Add decision of the DataStore
        decisions.add(DataStoreDecision.of(message.senderId, message.decision));

        // Data stores affected in current transaction
        final Set<ActorMetadata> affectedDataStores = dataStoresAffectedInTransaction.getOrDefault(message.transactionId, new HashSet<>());

        // Check if the number of decisions has reached all affected DataStore(s)
        if (decisions.size() == affectedDataStores.size()) {
            final TwoPcDecision decision;

            // Obtain final decision if commit or abort
            if (canCommit(decisions)) {
                decision = TwoPcDecision.COMMIT;
                LOGGER.info("Coordinator {} decided to commit transaction {}: {}", id, message.transactionId, decisions);
            } else {
                decision = TwoPcDecision.ABORT;
                LOGGER.info("Coordinator {} decided to abort transaction {}: {}", id, message.transactionId, decisions);
            }

            // Terminate transaction
            terminateTransaction(message.transactionId, decision);
        }
    }

    /**
     * Callback for {@link SnapshotMessage} message.
     *
     * @param message Received message
     */
    private void onSnapshotMessage(SnapshotMessage message) {
        LOGGER.debug("Coordinator {} received SnapshotMessage: {}", id, message);
        LOGGER.trace("Coordinator {} start snapshot {} involving {} DataStore(s)", id, message.snapshotId, dataStores.size());

        // Send snapshot message to all DataStore(s) snapshot request
        final SnapshotMessage outMessage = new SnapshotMessage(id, message.snapshotId);
        dataStores.forEach(dataStore -> {
            LOGGER.trace("Coordinator {} send to DataStore {} SnapshotMessage: {}", id, dataStore.id, outMessage);
            dataStore.ref.tell(outMessage, getSelf());
        });

        LOGGER.debug("Coordinator {} successfully sent snapshot request to {} DataStore(s)", id, dataStores.size());
    }

    /**
     * Callback for {@link SnapshotResultMessage} message.
     *
     * @param message Received message
     */
    private void onSnapshotResultMessage(SnapshotResultMessage message) {
        LOGGER.debug("Coordinator {} received from DataStore {} SnapshotResultMessage: {}", id, message.senderId, message);

        // Add received snapshot to saved snapshot
        snapshot.putAll(message.storage);

        // Check if all snapshots have been received
        if (snapshot.size() == (dataStores.size() * 10)) {
            // Print snapshot
            LOGGER.info("Coordinator {} snapshot {}: {}", id, message.snapshotId, JsonUtil.GSON.toJson(snapshot));

            // Calculate total Items value sum
            final int totalSum = snapshot.values().stream().mapToInt(item -> item.value).reduce(0, Integer::sum);
            // Check if totalSum is valid
            if (totalSum == snapshot.size() * 100) {
                LOGGER.info("Coordinator {} snapshot {} sum {} is VALID", id, message.snapshotId, totalSum);
            } else {
                LOGGER.info("Coordinator {} snapshot {} sum {} is INVALID", id, message.snapshotId, totalSum);
            }

            // Clean snapshot
            snapshot.clear();
        }
    }
}
