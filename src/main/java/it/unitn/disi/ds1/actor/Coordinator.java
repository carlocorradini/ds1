package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.message.twopc.*;
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
import it.unitn.disi.ds1.message.txn.TxnBeginResultMessage;
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
     * Number of {@link DataStore DataStore(s)} that has decided to COMMIT for {@link UUID transaction}.
     */
    private final Map<UUID, Integer> transactionDecisions;

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
                .match(TwoPcVoteResultMessage.class, this::onTwoPcVoteResultMessage)
                .match(TwoPcDecisionRequestMessage.class, this::onTwoPcDecisionRequestMessage)
                .match(TwoPcRecoveryMessage.class, this::onTwoPcRecoveryMessage)
                .match(TwoPcTimeoutMessage.class, this::onTwoPcTimeoutMessage)
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
        for (ActorMetadata metadata : dataStores) {
            if (metadata.id == dataStoreId)
                return metadata;
        }
        throw new NullPointerException(String.format("Coordinator %d is unable to obtain DataStore %d via Item key %d", id, dataStoreId, key));

        /* TODO
        return dataStores.stream()
                .filter(dataStore -> dataStore.id == dataStoreId)
                .findFirst()
                .orElseThrow(() -> new NullPointerException(String.format("Coordinator %d is unable to obtain DataStore %d via Item key %d", id, dataStoreId, key)));
                */
    }

    /**
     * Terminate the {@link UUID transaction} with the chosen final decision.
     *
     * @param transactionId {@link UUID Transaction} id
     * @param crash         Crash state enabled
     */
    private void terminateTransaction(UUID transactionId, boolean crash) {
        // Cancel timeout
        unTimeout(transactionId);

        // Obtain final decision
        final Decision decision = Optional.ofNullable(finalDecisions.get(transactionId))
                .orElseThrow(() -> new IllegalStateException(String.format("Coordinator %d tried to terminate transaction %s without a final decision", id, transactionId)));

        // Data stores affected in current transaction
        final Set<ActorMetadata> affectedDataStores = dataStoresAffectedInTransaction.getOrDefault(transactionId, new HashSet<>());

        // Communicate decision to all affected DataStore(s), if any
        final TwoPcDecisionMessage outMessageToDataStore = new TwoPcDecisionMessage(id, transactionId, decision);
        LOGGER.debug("Coordinator {} send to {} affected DataStore(s) to {} transaction {} TwoPcDecisionMessage: {}", id, affectedDataStores.size(), decision, transactionId, outMessageToDataStore);
        if (!crash || !Config.CRASH_ENABLED || (!Config.CRASH_COORDINATOR_DECISION_FIRST && !Config.CRASH_COORDINATOR_DECISION_ALL)) {
            // No crash
            multicast(affectedDataStores, outMessageToDataStore);
        } else if (Config.CRASH_COORDINATOR_DECISION_FIRST) {
            // Crash after first decision
            multicast(affectedDataStores, outMessageToDataStore, Crash.AFTER_FIRST_MESSAGE);
        } else if (Config.CRASH_COORDINATOR_DECISION_ALL) {
            // Crash after all decision(s)
            multicast(affectedDataStores, outMessageToDataStore, Crash.AFTER_ALL_MESSAGES);
        } else {
            throw new IllegalStateException(String.format("Coordinator %d unknown crash configuration state", id));
        }

        // Stop execution if crash enabled
        if (crash && Config.CRASH_ENABLED) return;

        // Communicate commit decision to Client
        transactionIdToClient.computeIfPresent(transactionId, (ignored, metadata) -> {
            final TxnEndResultMessage outMessageToClient = new TxnEndResultMessage(id, decision);
            metadata.ref.tell(outMessageToClient, getSelf());
            LOGGER.debug("Coordinator {} send to Client {} that transaction {} is {} TxnEndResultMessage: {}", id, metadata.id, transactionId, decision, outMessageToClient);
            return metadata;
        });

        // Clean resources
        cleanResources(transactionId);
    }

    /**
     * Terminate the {@link UUID transaction} with the chosen final decision.
     *
     * @param transactionId {@link UUID Transaction} id
     */
    private void terminateTransaction(UUID transactionId) {
        terminateTransaction(transactionId, false);
    }

    /**
     * Clean all resources that involves {@link UUID transaction}.
     *
     * @param transactionId Transaction id
     */
    private void cleanResources(UUID transactionId) {
        if (transactionId == null) return;

        unTimeout(transactionId);
        dataStoresAffectedInTransaction.remove(transactionId);
        transactionIdToClient.remove(transactionId);
        clientIdToTransactionId.values().remove(transactionId);
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

        // Simulate delay
        sleep();

        // Generate a transaction id and store all relevant data
        final UUID transactionId = UUID.randomUUID();
        clientIdToTransactionId.put(message.senderId, transactionId);
        transactionIdToClient.put(transactionId, ActorMetadata.of(message.senderId, getSender()));
        dataStoresAffectedInTransaction.put(transactionId, new HashSet<>());

        // Inform Client that the transaction has been accepted
        final TxnBeginResultMessage outMessage = new TxnBeginResultMessage(id);
        getSender().tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to Client {} involving transaction transactionId {} TxnBeginResultMessage: {}", id, message.senderId, transactionId, outMessage);
    }

    /**
     * Callback for {@link TxnReadMessage} message.
     *
     * @param message Received message
     */
    private void onTxnReadMessage(TxnReadMessage message) {
        LOGGER.debug("Coordinator {} received from Client {} TxnReadMessage: {}", id, message.senderId, message);

        // Simulate delay
        sleep();

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.senderId);

        // Obtain correct DataStore
        final ActorMetadata dataStore = dataStoreByItemKey(message.key);

        // Add DataStore to affected in transaction
        if (dataStoresAffectedInTransaction.get(transactionId).add(dataStore)) {
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

        // Simulate delay
        sleep();

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

        // Simulate delay
        sleep();

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.senderId);

        // Obtain correct DataStore
        final ActorMetadata dataStore = dataStoreByItemKey(message.key);

        // Add DataStore to affected in transaction
        if (dataStoresAffectedInTransaction.get(transactionId).add(dataStore)) {
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

        // Simulate delay
        sleep();

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

                    // Schedule timeout
                    timeout(transactionId, Config.TWOPC_COORDINATOR_TIMEOUT_MS);

                    // Send vote request to all affected DataStore(s) in transaction
                    final TwoPcVoteMessage outMessage = new TwoPcVoteMessage(id, transactionId, Decision.COMMIT);
                    LOGGER.debug("Coordinator {} send to {} affected DataStore(s) if can COMMIT transaction {} TwoPcVoteMessage: {}", id, affectedDataStores.size(), transactionId, outMessage);
                    if (!Config.CRASH_ENABLED || (!Config.CRASH_COORDINATOR_VOTE_FIRST && !Config.CRASH_COORDINATOR_VOTE_ALL)) {
                        // No crash
                        multicast(affectedDataStores, outMessage);
                    } else if (Config.CRASH_COORDINATOR_VOTE_FIRST) {
                        // Crash after first vote
                        multicast(affectedDataStores, outMessage, Crash.AFTER_FIRST_MESSAGE);
                    } else if (Config.CRASH_COORDINATOR_VOTE_ALL) {
                        // Crash after all vote(s)
                        multicast(affectedDataStores, outMessage, Crash.AFTER_ALL_MESSAGES);
                    } else {
                        throw new IllegalStateException(String.format("Coordinator %d unknown crash configuration state", id));
                    }
                } else {
                    // No DataStore(s) affected
                    LOGGER.warn("Coordinator {} no DataStore(s) are affected in transaction {}", id, transactionId);
                    terminateTransaction(transactionId);
                }
                break;
            }
            case ABORT: {
                // Client decided to abort
                LOGGER.info("Coordinator {} informed that Client {} want to ABORT transaction {}", id, message.senderId, transactionId);
                // Store final decision
                decide(transactionId, Decision.ABORT);
                terminateTransaction(transactionId);
                break;
            }
        }
    }

    /**
     * Callback for {@link TwoPcVoteResultMessage} message.
     *
     * @param message Received message
     */
    private void onTwoPcVoteResultMessage(TwoPcVoteResultMessage message) {
        // Simulate delay
        sleep();

        // Check if already decided
        if (hasDecided(message.transactionId)) {
            LOGGER.warn("Coordinator {} received from DataStore {} TwoPcVoteResultMessage when has already decided to {} for transaction {}", id, message.senderId, finalDecisions.get(message.transactionId), message.transactionId);
            return;
        }

        LOGGER.debug("Coordinator {} received from DataStore {} TwoPcVoteResultMessage: {}", id, message.senderId, message);

        // Check if the vote is ABORT, terminate transaction
        if (message.decision == Decision.ABORT) {
            LOGGER.info("Coordinator {} received from DataStore {} the vote to ABORT for transaction {}", id, message.senderId, message.transactionId);
            LOGGER.info("Coordinator {} decided to ABORT transaction {}", id, message.transactionId);
            decide(message.transactionId, Decision.ABORT);
            terminateTransaction(message.transactionId, true);
            return;
        }

        // Increment or create counter decisions
        final int decisions = transactionDecisions.compute(message.transactionId, (k, v) -> v != null ? v + 1 : 1);

        // Data stores affected in current transaction
        final Set<ActorMetadata> affectedDataStores = dataStoresAffectedInTransaction.getOrDefault(message.transactionId, new HashSet<>());

        // Check if counter decision has reached all affected DataStore(s)
        if (decisions == affectedDataStores.size()) {
            // All voted to COMMIT, COMMIT
            final Decision decision = Decision.COMMIT;
            LOGGER.info("Coordinator {} decided to COMMIT transaction {}", id, message.transactionId);

            // Store final decision
            decide(message.transactionId, decision);

            // Terminate transaction
            terminateTransaction(message.transactionId, true);
        }
    }

    /**
     * Callback for {@link TwoPcDecisionRequestMessage} message.
     *
     * @param message Received message
     */
    private void onTwoPcDecisionRequestMessage(TwoPcDecisionRequestMessage message) {
        LOGGER.debug("Coordinator {} received from DataStore {} TwoPcDecisionRequestMessage: {}", id, message.senderId, message);

        // Simulate delay
        sleep();

        // Check if it knows the final decision
        if (hasDecided(message.transactionId)) {
            LOGGER.debug("Coordinator {} DataStore {} does not know the final decision {}", id, message.senderId, finalDecisions.get(message.transactionId));
            // Terminate transaction
            terminateTransaction(message.transactionId);
        }
    }

    /**
     * Callback for {@link TwoPcRecoveryMessage} message.
     *
     * @param message Received message
     */
    @Override
    protected void onTwoPcRecoveryMessage(TwoPcRecoveryMessage message) {
        LOGGER.debug("Coordinator {} received TwoPcRecoveryMessage", id);

        // Simulate delay
        sleep();

        // Become normal
        getContext().become(createReceive());
        LOGGER.info("Coordinator {} recovering from crash", id);

        // Fix final decision(s)
        dataStoresAffectedInTransaction
                .keySet()
                .forEach((transactionId) -> {
                    if (!hasDecided(transactionId)) {
                        // Crashed before final decision
                        LOGGER.debug("Coordinator {} is recovering and has not decided yet for transaction {}: ABORT", id, transactionId);

                        // Decide to ABORT
                        decide(transactionId, Decision.ABORT);
                    } else {
                        // Crashed after final decision
                        final Decision decision = finalDecisions.getOrDefault(transactionId, Decision.ABORT);
                        LOGGER.debug("Coordinator {} is recovering and has already decided for transaction {}: {}", id, transactionId, decision);
                    }
                });

        // Terminate transaction(s)
        for (final UUID transactionId : List.copyOf(dataStoresAffectedInTransaction.keySet())) {
            terminateTransaction(transactionId, Config.CRASH_COORDINATOR_ON_RECOVERY);
        }
    }


    /**
     * Callback for {@link TwoPcTimeoutMessage} message.
     *
     * @param message Received message
     */
    @Override
    protected void onTwoPcTimeoutMessage(TwoPcTimeoutMessage message) {
        LOGGER.debug("Coordinator {} received TwoPcTimeoutMessage: {}", id, message);

        // Simulate delay
        sleep();

        // Clear the timeout for transaction
        unTimeout(message.transactionId);

        if (!hasDecided(message.transactionId)) {
            LOGGER.info("Coordinator {} in timeout has not decided yet for transaction {}", id, message.transactionId);

            // Decide to ABORT
            decide(message.transactionId, Decision.ABORT);
            LOGGER.debug("Coordinator {} in timeout unilaterally ABORT for transaction {}", id, message.transactionId);
            // Terminate transaction
            terminateTransaction(message.transactionId, true);
        } else {
            final Decision decision = finalDecisions.get(message.transactionId);
            LOGGER.info("Coordinator {} in timeout has already decided to {} for transaction {}", id, decision, message.transactionId);
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
            final int totalSum = snapshot.values().stream().mapToInt(Item::getValue).reduce(0, Integer::sum);
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
