package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.DataStoreDecision;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.exception.TransactionIsRunningException;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecisionMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcVoteResponseMessage;
import it.unitn.disi.ds1.message.snap.SnapshotTokenMessage;
import it.unitn.disi.ds1.message.snap.SnapshotTokenResultMessage;
import it.unitn.disi.ds1.message.snap.StartSnapshotMessage;
import it.unitn.disi.ds1.message.txn.TxnEndMessage;
import it.unitn.disi.ds1.message.txn.TxnResultMessage;
import it.unitn.disi.ds1.message.op.read.ReadResultMessage;
import it.unitn.disi.ds1.message.op.read.ReadCoordinatorMessage;
import it.unitn.disi.ds1.message.op.read.ReadMessage;
import it.unitn.disi.ds1.message.op.read.ReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.op.write.WriteCoordinatorMessage;
import it.unitn.disi.ds1.message.op.write.WriteMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcVoteRequestMessage;
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
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .serializeNulls()
            .create();

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
                .match(ReadMessage.class, this::onReadMessage)
                .match(ReadResultCoordinatorMessage.class, this::onReadResultCoordinatorMessage)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(TxnEndMessage.class, this::onTxnEndMessage)
                .match(TwoPcVoteResponseMessage.class, this::onTwoPcVoteResponseMessage)
                .match(StartSnapshotMessage.class, this::onStartSnapshotMessage)
                .match(SnapshotTokenResultMessage.class, this::onSnapshotTokenResultMessage)
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
        final TxnResultMessage outMessageToClient = new TxnResultMessage(decision);
        client.ref.tell(outMessageToClient, getSender());
        LOGGER.debug("Coordinator {} send to Client {} that transaction {} is {} TxnResultMessage: {}", id, client.id, transactionId, decision, outMessageToClient);

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
    private void onTxnBeginMessage(TxnBeginMessage message) throws TransactionIsRunningException {
        LOGGER.debug("Coordinator {} received from Client {} TxnBeginMessage: {}", id, message.clientId, message);

        // Check if Client has already a transaction running
        final UUID transactionIdRunning = clientIdToTransactionId.get(message.clientId);
        if (transactionIdRunning != null)
            throw new TransactionIsRunningException(String.format("Coordinator %d received a TxnBeginMessage from Client %d when transaction %s is running", id, message.clientId, transactionIdRunning));

        // Generate a transaction id and store all relevant data
        final UUID transactionId = UUID.randomUUID();
        clientIdToTransactionId.put(message.clientId, transactionId);
        transactionIdToClient.put(transactionId, ActorMetadata.of(message.clientId, getSender()));

        // Inform Client that the transaction has been accepted
        final TxnAcceptMessage outMessage = new TxnAcceptMessage();
        getSender().tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to Client {} involving transaction transactionId {} TxnAcceptMessage: {}", id, message.clientId, transactionId, outMessage);
    }

    /**
     * Callback for {@link ReadMessage} message.
     *
     * @param message Received message
     */
    private void onReadMessage(ReadMessage message) {
        LOGGER.debug("Coordinator {} received from Client {} ReadMessage: {}", id, message.clientId, message);

        // Obtain correct DataStore
        final ActorMetadata dataStore = dataStoreByItemKey(message.key);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.clientId);

        // Add DataStore to affected in transaction
        final Set<ActorMetadata> dataStoresAffected = dataStoresAffectedInTransaction.computeIfAbsent(transactionId, k -> new HashSet<>());
        if (dataStoresAffected.add(dataStore)) {
            LOGGER.trace("Coordinator {} add DataStore {} to affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        } else {
            LOGGER.trace("Coordinator {} DataStore {} already present in affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        }

        // Send to DataStore Item read message
        final ReadCoordinatorMessage outMessage = new ReadCoordinatorMessage(id, transactionId, message.key);
        dataStore.ref.tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to DataStore {} ReadCoordinatorMessage: {}", id, dataStore.id, outMessage);
    }

    /**
     * Callback for {@link ReadResultCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onReadResultCoordinatorMessage(ReadResultCoordinatorMessage message) {
        LOGGER.debug("Coordinator {} received from DataStore {} ReadResultCoordinatorMessage: {}", id, message.dataStoreId, message);

        // Obtain Client
        final ActorMetadata client = transactionIdToClient.get(message.transactionId);

        // Send to Client Item read reply message
        final ReadResultMessage outMessage = new ReadResultMessage(message.key, message.value);
        client.ref.tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to Client {} ReadResultMessage: {}", id, client.id, outMessage);
    }

    /**
     * Callback for {@link WriteMessage} message.
     *
     * @param message Received message
     */
    private void onWriteMessage(WriteMessage message) {
        LOGGER.debug("Coordinator {} received from Client {} WriteMessage: {}", id, message.clientId, message);

        // Obtain correct DataStore
        final ActorMetadata dataStore = dataStoreByItemKey(message.key);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.clientId);

        // Add DataStore to affected in transaction
        final Set<ActorMetadata> dataStoresAffected = dataStoresAffectedInTransaction.computeIfAbsent(transactionId, k -> new HashSet<>());
        if (dataStoresAffected.add(dataStore)) {
            LOGGER.trace("Coordinator {} add DataStore {} to affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        } else {
            LOGGER.trace("Coordinator {} DataStore {} already present in affected DataStore(s) for transaction {}", id, dataStore.id, transactionId);
        }

        // Send to DataStore Item write message
        final WriteCoordinatorMessage outMessage = new WriteCoordinatorMessage(id, transactionId, message.key, message.value);
        dataStore.ref.tell(outMessage, getSelf());
        LOGGER.debug("Coordinator {} send to DataStore {} WriteCoordinatorMessage: {}", id, dataStore.id, outMessage);
    }

    /**
     * Callback for {@link TxnEndMessage} message.
     *
     * @param message Received message
     */
    private void onTxnEndMessage(TxnEndMessage message) {
        LOGGER.debug("Coordinator {} received from Client {} TxnEndMessage {}", id, message.clientId, message);

        // Obtain transaction id
        final UUID transactionId = clientIdToTransactionId.get(message.clientId);

        // Check Client decision
        switch (message.decision) {
            case COMMIT: {
                // Client decided to commit
                LOGGER.info("Coordinator {} informed that Client {} want to COMMIT transaction {}", id, message.clientId, transactionId);
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
                    terminateTransaction(transactionId, TwoPcDecision.COMMIT);
                }
                break;
            }
            case ABORT: {
                // Client decided to abort
                LOGGER.info("Coordinator {} informed that Client {} want to ABORT transaction {}", id, message.clientId, transactionId);
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
        LOGGER.debug("Coordinator {} received from DataStore {} TwoPcVoteResponseMessage: {}", id, message.dataStoreId, message);

        // Obtain or create DataStore(s) decisions
        final Set<DataStoreDecision> decisions = transactionDecisions.computeIfAbsent(message.transactionId, k -> new HashSet<>());
        // Add decision of the DataStore
        decisions.add(DataStoreDecision.of(message.dataStoreId, message.decision));

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
     * Callback for {@link StartSnapshotMessage} message.
     *
     * @param message Received message
     */
    private void onStartSnapshotMessage(StartSnapshotMessage message) {
        LOGGER.debug("Coordinator {} received StartSnapshotMessage: {}", id, message);
        LOGGER.trace("Coordinator {} start snapshot {} involving {} DataStore(s)", id, message.snapshotId, dataStores.size());

        // Send to all DataStore(s) snapshot request
        final SnapshotTokenMessage outMessage = new SnapshotTokenMessage(id, message.snapshotId);
        dataStores.forEach(dataStore -> {
            LOGGER.trace("Coordinator {} send to DataStore {} SnapshotTokenMessage: {}", id, dataStore.id, outMessage);
            dataStore.ref.tell(outMessage, getSelf());
        });

        LOGGER.debug("Coordinator {} successfully sent snapshot request to {} DataStore(s)", id, dataStores.size());
    }

    /**
     * Callback for {@link SnapshotTokenResultMessage} message.
     *
     * @param message Received message
     */
    private void onSnapshotTokenResultMessage(SnapshotTokenResultMessage message) {
        LOGGER.debug("Coordinator {} received from DataStore {} StartSnapshotMessage: {}", id, message.dataStoreId, message);

        // Add all storage from current DataStore to snapshot
        snapshot.putAll(message.storage);

        // Check if all snapshots have been received
        if (snapshot.size() == (dataStores.size() * 10)) {
            // Print snapshot
            LOGGER.info("Coordinator {} snapshot {}: {}", id, message.snapshotId, GSON.toJson(snapshot));

            // Calculate total Items value sum
            final int totalSum = snapshot.values().stream().mapToInt(item -> item.value).reduce(0, Integer::sum);
            // Check if totalSum is valid
            assert snapshot.size() * 100 == dataStores.size() * 10 * 100;
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
