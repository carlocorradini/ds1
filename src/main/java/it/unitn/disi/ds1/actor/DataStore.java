package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.txn.read.TxnReadCoordinatorMessage;
import it.unitn.disi.ds1.message.txn.read.TxnReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.txn.write.TxnWriteCoordinatorMessage;
import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.message.twopc.TwoPcDecisionMessage;
import it.unitn.disi.ds1.message.twopc.TwoPcVoteMessage;
import it.unitn.disi.ds1.message.twopc.TwoPcVoteResultMessage;
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
     * @return True if can commit, false otherwise
     */
    private synchronized boolean canCommit(UUID transactionId) {
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
    private synchronized boolean checkItemsVersion(UUID transactionId) {
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
    private synchronized boolean lockItems(UUID transactionId) {
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
    private synchronized void cleanLockItems(UUID transactionId) {
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

        // Clean possible locked Item(s)
        cleanLockItems(transactionId);
        // Clean private workspace
        workspaces.remove(transactionId);
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
    }

    /**
     * Callback for {@link TxnWriteCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onTxnWriteCoordinatorMessage(TxnWriteCoordinatorMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} TxnWriteCoordinatorMessage: {}", id, message.senderId, message);

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

        final boolean canCommit;

        // Check client decision
        switch (message.decision) {
            case COMMIT: {
                // Check if transaction can commit
                canCommit = canCommit(message.transactionId);
                LOGGER.debug("DataStore {} received COMMIT decision from Coordinator {} involving transaction {} and decision is {}", id, message.senderId, message.transactionId, Decision.valueOf(canCommit));

                // Send response to Coordinator
                final TwoPcVoteResultMessage outMessage = new TwoPcVoteResultMessage(id, message.transactionId, Decision.valueOf(canCommit));
                getSender().tell(outMessage, getSender());
                LOGGER.debug("DataStore {} send to Coordinator {} TwoPcVoteResultMessage: {}", id, message.senderId, outMessage);
                break;
            }
            case ABORT:
                throw new IllegalStateException(String.format("DataStore %d received ABORT decision from Coordinator %d involving transaction %s", id, message.senderId, message.transactionId));
            default:
                throw new IllegalStateException(String.format("DataStore %d received UNKNOWN decision from Coordinator %d involving transaction %s", id, message.senderId, message.transactionId));
        }
    }

    /**
     * Callback for {@link TwoPcDecisionMessage message}.
     *
     * @param message Received message
     */
    private void onTwoPcDecisionMessage(TwoPcDecisionMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} to {} TwoPcDecisionMessage: {}", id, message.senderId, message.decision, message);

        // If decision is to commit, let's commit
        if (message.decision == Decision.COMMIT) {
            synchronized (this) {
                // Obtain private workspace of the transaction
                final Map<Integer, Item> workspace = workspaces.get(message.transactionId);
                // Commit
                // FIXME Rimuovere operation
                storage.putAll(workspaces.get(message.transactionId));
                LOGGER.info("DataStore {} successfully committed transaction {}: {}", id, message.transactionId, JsonUtil.GSON.toJson(workspace));
            }
        }

        // Clean resources
        cleanResources(message.transactionId);
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
