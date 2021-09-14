package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.message.twopc.*;
import it.unitn.disi.ds1.message.txn.read.TxnReadCoordinatorMessage;
import it.unitn.disi.ds1.message.txn.read.TxnReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.txn.write.TxnWriteCoordinatorMessage;
import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.message.snapshot.SnapshotMessage;
import it.unitn.disi.ds1.message.snapshot.SnapshotResultMessage;
import it.unitn.disi.ds1.message.welcome.DataStoreWelcomeMessage;
import it.unitn.disi.ds1.util.JsonUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Data Store {@link Actor actor} class.
 */
public final class DataStore extends Actor {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(DataStore.class);

    /**
     * {@link Item} default value.
     */
    private static final int ITEM_DEFAULT_VALUE = 100;

    /**
     * {@link Item} default version.
     */
    private static final int ITEM_DEFAULT_VERSION = 0;

    /**
     * {@link DataStore DataStore(s)} metadata.
     */
    private final List<ActorMetadata> dataStores;

    /**
     * Storage used for persistence.
     */
    private final Map<Integer, Item> storage;

    /**
     * Private workspace for each transaction.
     * Key is the transaction id.
     * Value is all the modified Item(s) identified by the key.
     */
    private final Map<UUID, Map<Integer, Item>> workspaces;

    /**
     * Storage used for COMMIT/ABORT taken by {@link DataStore} for each transaction.
     */
    private final Map<UUID, Decision> transactionVotes;

    /**
     * Storage used for saving {@link Coordinator} linked to transaction.
     */
    private final Map<UUID, ActorMetadata> transactionIdToCoordinator;

    // --- Constructors ---

    /**
     * Construct a new Data Store class.
     *
     * @param id Data Store id
     */
    public DataStore(int id) {
        super(id);
        this.dataStores = new ArrayList<>();
        this.storage = new HashMap<>();
        this.workspaces = new HashMap<>();
        this.transactionVotes = new HashMap<>();
        this.transactionIdToCoordinator = new HashMap<>();

        // Initialize items
        IntStream.range(id * 10, (id * 10) + 10).forEach(i -> storage.put(i, new Item(ITEM_DEFAULT_VALUE, ITEM_DEFAULT_VERSION)));

        LOGGER.debug("DataStore {} initialized", id);
    }

    /**
     * Return Data Store {@link Props}.
     *
     * @param id Data Store id
     * @return Data Store {@link Props}
     */
    public static Props props(int id) {
        return Props.create(DataStore.class, () -> new DataStore(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DataStoreWelcomeMessage.class, this::onDataStoreWelcomeMessage)
                .match(TxnReadCoordinatorMessage.class, this::onTxnReadCoordinatorMessage)
                .match(TxnWriteCoordinatorMessage.class, this::onTxnWriteCoordinatorMessage)
                .match(TwoPcVoteMessage.class, this::onTwoPcVoteMessage)
                .match(TwoPcDecisionMessage.class, this::onTwoPcDecisionMessage)
                .match(TwoPcDecisionRequestMessage.class, this::onTwoPcDecisionRequestMessage)
                .match(TwoPcRecoveryMessage.class, this::onTwoPcRecoveryMessage)
                .match(TwoPcTimeoutMessage.class, this::onTwoPcTimeoutMessage)
                .match(SnapshotMessage.class, this::onSnapshotMessage)
                .build();
    }

    // --- Methods ---

    /**
     * Check if {@link UUID transaction} is able to commit.
     * Checks:
     * 1. Items version match
     * 2. Successfully locked items
     *
     * @param transactionId {@link UUID Transaction} id
     * @return True if it can commit, false otherwise
     */
    private boolean canCommit(UUID transactionId) {
        if (!lockItems(transactionId)) {
            LOGGER.warn("DataStore {} lock Item(s) in transaction {} has FAILED: {}", id, transactionId, JsonUtil.GSON.toJson(workspaces.get(transactionId)));
            return false;
        }
        LOGGER.debug("DataStore {} lock Item(s) in transaction {} is SUCCESSFUL", id, transactionId);
        if (!checkItemsVersion(transactionId)) {
            LOGGER.warn("DataStore {} check Item(s) version in transaction {} has FAILED: {}", id, transactionId, JsonUtil.GSON.toJson(workspaces.get(transactionId)));
            return false;
        }
        LOGGER.debug("DataStore {} check Item(s) version in transaction {} is SUCCESSFUL", id, transactionId);

        // Able to commit
        LOGGER.debug("DataStore {} can successfully commit transaction {}", id, transactionId);
        return true;
    }

    /**
     * Return true if version of the {@link Item Item(s)} in storage and workspace of {@link UUID transaction} match, otherwise false.
     *
     * @param transactionId Transaction id
     * @return True if matched, false otherwise
     */
    private boolean checkItemsVersion(UUID transactionId) {
        return workspaces.get(transactionId)
                .entrySet().stream()
                .allMatch((entry) -> {
                    final Item itemInWorkSpace = entry.getValue();
                    final Item itemInStorage = storage.get(entry.getKey());

                    final boolean isValid;
                    if (!itemInWorkSpace.isValueChanged()) {
                        // READ
                        isValid = itemInWorkSpace.getVersion() == itemInStorage.getVersion()
                                && itemInWorkSpace.getValue() == itemInStorage.getValue();
                        if (!isValid) {
                            LOGGER.debug("DataStore {} READ check for Item {} in transaction {} is INVALID", id, entry.getKey(), transactionId);
                        }
                    } else {
                        // WRITE
                        isValid = itemInWorkSpace.getVersion() == itemInStorage.getVersion() + 1;
                        if (!isValid) {
                            LOGGER.debug("DataStore {} WRITE check for Item {} in transaction {} is INVALID", id, entry.getKey(), transactionId);
                        }
                    }

                    return isValid;
                });
    }

    /**
     * Return true if {@link UUID transaction} is able to lock all {@link Item Item(s)} involved, otherwise false.
     *
     * @param transactionId Transaction id
     * @return True if all {@link Item Item(s)} are successfully locked, false otherwise
     */
    private boolean lockItems(UUID transactionId) {
        // Obtain private workspace of the transaction
        final Map<Integer, Item> workspace = workspaces.get(transactionId);

        // Try to lock all Item(s) in storage involved in transaction
        final boolean locked = workspace.entrySet().stream()
                .allMatch(entry -> storage.get(entry.getKey()).lock(transactionId));

        // If operation failed, clean lock in storage
        if (!locked) cleanLockItems(transactionId);

        return locked;
    }

    /**
     * Clean possible locked {@link Item Item(s)} that are involved in the {@link UUID transaction}.
     *
     * @param transactionId {@link UUID Transaction} id
     */
    private void cleanLockItems(UUID transactionId) {
        workspaces.get(transactionId)
                .forEach((key, value) -> storage.get(key).unlock(transactionId));
    }

    /**
     * Clean all resources that involves {@link UUID transaction}.
     *
     * @param transactionId Transaction id
     */
    private void cleanResources(UUID transactionId) {
        if (transactionId == null) return;

        cleanLockItems(transactionId);
        unTimeout(transactionId);
        workspaces.remove(transactionId);
        transactionVotes.remove(transactionId);
        transactionIdToCoordinator.remove(transactionId);
        LOGGER.trace("DataStore {} clean resources involving transaction {}", id, transactionId);
    }

    // --- Message handlers ---

    /**
     * Callback for {@link DataStoreWelcomeMessage} message.
     *
     * @param message Received message.
     */
    private void onDataStoreWelcomeMessage(DataStoreWelcomeMessage message) {
        LOGGER.debug("Coordinator {} received CoordinatorWelcomeMessage: {}", id, message);

        dataStores.clear();
        dataStores
                .addAll(message.dataStores.stream().filter(dataStore -> dataStore.id != id).collect(Collectors.toCollection(ArrayList::new)));
    }

    /**
     * Callback for {@link TxnReadCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onTxnReadCoordinatorMessage(TxnReadCoordinatorMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} TxnReadCoordinatorMessage: {}", id, message.senderId, message);

        // Simulate delay
        sleep();

        // Obtain private workspace, otherwise create
        final Map<Integer, Item> workspace = workspaces.computeIfAbsent(message.transactionId, k -> new HashMap<>());

        // Obtain Item in storage
        final Item itemInStorage = storage.get(message.key);

        // Obtain Item in workspace, compute it if absent
        final Item itemInWorkspace = workspace.computeIfAbsent(message.key, k -> {
            final Item item = new Item(itemInStorage.getValue(), itemInStorage.getVersion());
            LOGGER.trace("DataStore {} on READ added Item {} involving transaction {} to workspace: {}", id, message.key, message.transactionId, item);
            return item;
        });

        // Respond to Coordinator with Item
        final TxnReadResultCoordinatorMessage outMessage = new TxnReadResultCoordinatorMessage(id, message.transactionId, message.key, itemInWorkspace.getValue());
        getSender().tell(outMessage, getSelf());
        LOGGER.debug("DataStore {} send to Coordinator {} TxnReadResultCoordinatorMessage: {}", id, message.senderId, outMessage);

        // Store coordinator with its transaction id
        transactionIdToCoordinator.put(message.transactionId, new ActorMetadata(message.senderId, getSender()));
    }

    /**
     * Callback for {@link TxnWriteCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onTxnWriteCoordinatorMessage(TxnWriteCoordinatorMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} TxnWriteCoordinatorMessage: {}", id, message.senderId, message);

        // Simulate delay
        sleep();

        // Obtain private workspace, otherwise create
        final Map<Integer, Item> workspace = workspaces.computeIfAbsent(message.transactionId, k -> new HashMap<>());

        // Obtain Item in storage
        final Item itemInStorage = storage.get(message.key);

        // Compute Item in workspace
        final Item itemInWorkspace = workspace.compute(message.key, (k, oldItemInWorkspace) -> {
            final Item item = oldItemInWorkspace == null ? new Item(message.value, itemInStorage.getVersion()) : oldItemInWorkspace;
            item.setValue(message.value);
            LOGGER.trace("DataStore {} on WRITE added Item {} involving transaction {} to workspace: {}", id, message.key, message.transactionId, item);
            return item;
        });

        LOGGER.trace("DataStore {} TxnWriteCoordinatorMessage item {} in transaction {} added to workspace: {}", id, message.key, message.transactionId, itemInWorkspace);
    }

    /**
     * Callback for {@link TwoPcVoteMessage} message.
     *
     * @param message Received message
     */
    private void onTwoPcVoteMessage(TwoPcVoteMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} TwoPcVoteMessage: {}", id, message.senderId, message);

        // Simulate delay
        sleep();

        // Check decision
        if (message.decision != Decision.COMMIT)
            throw new IllegalStateException(String.format("DataStore %d received %s decision from Coordinator %d involving transaction %s", id, message.decision, message.senderId, message.transactionId));

        // Compute vote if not already voted
        final Decision vote = transactionVotes.computeIfAbsent(message.transactionId, k -> {
            final Decision v = Decision.valueOf(canCommit(message.transactionId));
            LOGGER.debug("DataStore {} received COMMIT decision from Coordinator {} involving transaction {} and vote is {}", id, message.senderId, message.transactionId, v);
            return v;
        });

        // Crash before sending vote response to Coordinator
        if (Config.CRASH_DATA_STORE_VOTE) {
            LOGGER.debug("DataStore {} crash before sending vote {} response to Coordinator {}", id, vote, message.senderId);
            crash();
            return;
        }

        // Send response to Coordinator
        final TwoPcVoteResultMessage outMessage = new TwoPcVoteResultMessage(id, message.transactionId, vote);
        getSender().tell(outMessage, getSender());
        LOGGER.debug("DataStore {} send to Coordinator {} TwoPcVoteResultMessage: {}", id, message.senderId, outMessage);

        // Schedule timeout
        timeout(message.transactionId, Config.TWOPC_DATA_STORE_TIMEOUT_MS);

        // Crash before receiving decision from Coordinator
        if (Config.CRASH_DATA_STORE_DECISION) {
            LOGGER.debug("DataStore {} crash before receiving decision from Coordinator {}", id, message.senderId);
            crash();
        }
    }

    /**
     * Callback for {@link TwoPcDecisionMessage message}.
     *
     * @param message Received message
     */
    private void onTwoPcDecisionMessage(TwoPcDecisionMessage message) {
        LOGGER.debug("DataStore {} received from Actor {} to {} TwoPcDecisionMessage: {}", id, message.senderId, message.decision, message);
        if (hasDecided(message.transactionId)) {
            LOGGER.warn("DataStore {} already decided to {} for transaction {}", id, finalDecisions.get(message.transactionId), message.transactionId);
            return;
        }

        // Simulate delay
        sleep();

        // Clear the timeout for transaction
        unTimeout(message.transactionId);

        // Store final decision
        decide(message.transactionId, message.decision);

        // If decision is to commit, let's commit
        if (message.decision == Decision.COMMIT) {
            // Obtain private workspace of the transaction
            final Map<Integer, Item> workspace = workspaces.get(message.transactionId);
            // Commit
            workspace.forEach((key, item) -> storage.put(key, new Item(item.getValue(), item.getVersion())));
            LOGGER.info("DataStore {} successfully committed transaction {}: {}", id, message.transactionId, JsonUtil.GSON.toJson(workspace));
        }

        // Clean resources
        cleanResources(message.transactionId);
    }

    /**
     * Callback for {@link TwoPcDecisionRequestMessage} message.
     *
     * @param message Received message
     */
    private void onTwoPcDecisionRequestMessage(TwoPcDecisionRequestMessage message) {
        LOGGER.debug("DataStore {} received from Actor {} TwoPcDecisionRequest: {}", id, message.senderId, message);

        // Simulate delay
        sleep();

        // Check if it knows the final decision
        if (hasDecided(message.transactionId)) {
            // Obtain decision
            final Decision decision = finalDecisions.get(message.transactionId);
            final TwoPcDecisionMessage outMessage = new TwoPcDecisionMessage(id, message.transactionId, decision);
            getSender().tell(outMessage, getSelf());
            LOGGER.debug("DataStore {} send to another DataStore {} during 2PC decision request TwoPcDecisionMessage: {}", id, message.senderId, outMessage);
        }
    }

    /**
     * Callback for {@link TwoPcRecoveryMessage} message.
     *
     * @param message Received message
     */
    @Override
    protected void onTwoPcRecoveryMessage(TwoPcRecoveryMessage message) {
        LOGGER.debug("Data store {} received TwoPcRecoveryMessage", id);

        // Simulate delay
        sleep();

        // Become normal
        getContext().become(createReceive());
        LOGGER.info("DataStore {} recovering from crash", id);

        // Fix final decision(s)
        workspaces
                .keySet()
                .forEach(transactionId -> {
                    if (!transactionVotes.containsKey(transactionId)) {
                        // Not voted
                        LOGGER.debug("DataStore {} is recovering and has not voted yet for transaction {}", id, transactionId);
                        // Save ABORT vote
                        transactionVotes.put(transactionId, Decision.ABORT);
                        // Inform myself to ABORT as final decision
                        getSelf().tell(new TwoPcDecisionMessage(Message.NO_SENDER_ID, transactionId, Decision.ABORT), getSelf());
                        LOGGER.info("DataStore {} is recovering safely ABORT for transaction {}", id, transactionId);
                    } else if (!hasDecided(transactionId)) {
                        // Not decided
                        // Obtain coordinator
                        final ActorMetadata coordinator = transactionIdToCoordinator.get(transactionId);
                        // Out message
                        final TwoPcDecisionRequestMessage outMessage = new TwoPcDecisionRequestMessage(id, transactionId);
                        // Ask coordinator
                        coordinator.ref.tell(outMessage, getSelf());
                        // Schedule timeout
                        timeout(transactionId, Config.TWOPC_DATA_STORE_TIMEOUT_MS);
                        LOGGER.debug("DataStore {} is recovering and ask Coordinator {} for decision involving transaction {}: {}", id, coordinator.id, transactionId, outMessage);
                    } else {
                        // Already know the final decision
                        final Decision decision = finalDecisions.get(transactionId);
                        LOGGER.debug("DataStore {} is recovering and has already known the final decision: {} for transaction {}", id, decision, transactionId);
                    }
                });
    }

    /**
     * Callback for {@link TwoPcTimeoutMessage} message.
     *
     * @param message Received message
     */
    @Override
    protected void onTwoPcTimeoutMessage(TwoPcTimeoutMessage message) {
        LOGGER.debug("DataStore {} received TwoPcTimeoutMessage: {}", id, message);

        // Simulate delay
        sleep();

        // Clear the timeout for transaction
        unTimeout(message.transactionId);

        // Check if not voted
        if (!transactionVotes.containsKey(message.transactionId)) {
            // Not voted
            LOGGER.info("DataStore {} in timeout safely decide to ABORT because it has not voted yet", id);

            // Store the vote
            transactionVotes.put(message.transactionId, Decision.ABORT);

            // Timeout before vote
            final TwoPcDecisionMessage outMessage = new TwoPcDecisionMessage(Message.NO_SENDER_ID, message.transactionId, Decision.ABORT);
            getSelf().tell(outMessage, getSelf());
            LOGGER.debug("DataStore {} in timeout ABORT for transaction {}", id, message.transactionId);
        }

        // Check if not decided and voted COMMIT
        if (!hasDecided(message.transactionId) && transactionVotes.get(message.transactionId) == Decision.COMMIT) {
            LOGGER.info("DataStore {} in timeout voted COMMIT for transaction {} and ask around to know the final decision", id, message.transactionId);
            final TwoPcDecisionRequestMessage outMessage = new TwoPcDecisionRequestMessage(id, message.transactionId);
            multicast(dataStores, outMessage);
        } else {
            final Decision decision = hasDecided(message.transactionId) ? finalDecisions.get(message.transactionId) : transactionVotes.getOrDefault(message.transactionId, Decision.ABORT);
            LOGGER.info("DataStore {} in timeout voted {} for transaction {}", id, decision, message.transactionId);
        }
    }

    /**
     * Callback for {@link SnapshotMessage message}.
     *
     * @param message Received message
     */
    private void onSnapshotMessage(SnapshotMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} SnapshotMessage: {}", id, message.senderId, message);
        LOGGER.trace("DataStore {} generating snapshot {}: {}", id, message.snapshotId, storage);

        // Check if there are transactions running
        if (!workspaces.isEmpty())
            throw new IllegalStateException(String.format("DataStore %d is unable to create snapshot %d since there are %d transaction(s) running", id, message.snapshotId, workspaces.size()));

        // Send response to Coordinator
        final SnapshotResultMessage outMessage = new SnapshotResultMessage(id, message.snapshotId, storage);
        getSender().tell(outMessage, getSelf());
        LOGGER.debug("DataStore {} send to Coordinator {} SnapshotResultMessage: {}", id, message.senderId, outMessage);
    }
}
