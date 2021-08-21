package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.op.read.ReadCoordinatorMessage;
import it.unitn.disi.ds1.message.op.read.ReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.op.write.WriteCoordinatorMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecisionMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcVoteRequestMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcVoteResponseMessage;
import it.unitn.disi.ds1.message.snap.SnapshotTokenMessage;
import it.unitn.disi.ds1.message.snap.SnapshotTokenResultMessage;
import it.unitn.disi.ds1.message.welcome.DataStoreWelcomeMessage;
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
        IntStream.range(id * 10, (id * 10) + 10).forEach(i -> storage.put(i, new Item()));

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
                .match(ReadCoordinatorMessage.class, this::onReadCoordinatorMessage)
                .match(WriteCoordinatorMessage.class, this::onWriteCoordinatorMessage)
                .match(TwoPcVoteRequestMessage.class, this::onTwoPcVoteRequestMessage)
                .match(TwoPcDecisionMessage.class, this::onTwoPcDecisionMessage)
                .match(SnapshotTokenMessage.class, this::onSnapshotTokenMessage)
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
        if (!checkItemsVersion(transactionId)) {
            LOGGER.debug("DataStore {} check Item(s) version in transaction {} has failed", id, transactionId);
            return false;
        }
        LOGGER.debug("DataStore {} check Item(s) version in transaction {} is successful", id, transactionId);
        if (!lockItems(transactionId)) {
            LOGGER.debug("DataStore {} lock Item(s) in transaction {} has failed", id, transactionId);
            return false;
        }
        LOGGER.debug("DataStore {} lock Item(s) in transaction {} is successful", id, transactionId);

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
        // Obtain private workspace of the transaction
        final Map<Integer, Item> workspace = workspaces.get(transactionId);
        if (workspace == null) return false;

        return workspace.entrySet().stream().allMatch((entry) -> {
            final Item itemInWorkSpace = entry.getValue();
            final Item itemInStorage = storage.get(entry.getKey());

            return itemInWorkSpace.version == itemInStorage.version + 1;
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
        if (workspace == null) return false;

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
        // Obtain private workspace of the transaction
        final Map<Integer, Item> workspace = workspaces.get(transactionId);
        if (workspace == null) return;

        workspace.forEach((key, value) -> storage.get(key).unlockIfIsLocker(transactionId));
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
     * Callback for {@link ReadCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onReadCoordinatorMessage(ReadCoordinatorMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} ReadCoordinatorMessage: {}", id, message.coordinatorId, message);

        final Item item;

        // Obtain item for the current transaction
        final Map<Integer, Item> workspace = workspaces.get(message.transactionId);
        if (workspace != null && workspace.containsKey(message.key)) {
            // Item in workspace
            item = workspace.get(message.key);
            LOGGER.trace("DataStore {} ReadCoordinatorMessage item {} in transaction {} found in workspace: {}", id, message.key, message.transactionId, item);
        } else {
            // Item in storage
            item = storage.get(message.key);
            LOGGER.trace("DataStore {} ReadCoordinatorMessage item {} in transaction {} found in storage: {}", id, message.key, message.transactionId, item);
        }

        final ReadResultCoordinatorMessage outMessage = new ReadResultCoordinatorMessage(id, message.transactionId, message.key, item.value);
        getSender().tell(outMessage, getSelf());

        LOGGER.debug("DataStore {} send to Coordinator {} ReadResultCoordinatorMessage: {}", id, message.coordinatorId, outMessage);
    }

    /**
     * Callback for {@link WriteCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onWriteCoordinatorMessage(WriteCoordinatorMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} WriteCoordinatorMessage: {}", id, message.coordinatorId, message);

        // Obtain private workspace, otherwise create
        final Map<Integer, Item> workspace = workspaces.computeIfAbsent(message.transactionId, k -> new HashMap<>());

        // Add new item to workspace
        final Item itemInStorage = storage.get(message.key);
        final Item newItemInWorkspace = new Item(message.value, itemInStorage.version + 1);
        if (workspace.isEmpty()) {
            LOGGER.trace("DataStore {} add write request involving transaction {} in a new workspace: {}", id, message.transactionId, newItemInWorkspace);
        } else {
            LOGGER.trace("DataStore {} add write request involving transaction {} in an existing workspace: {}", id, message.transactionId, newItemInWorkspace);
        }
        workspace.put(message.key, newItemInWorkspace);
    }

    /**
     * Callback for {@link TwoPcVoteRequestMessage} message.
     *
     * @param message Received message
     */
    private void onTwoPcVoteRequestMessage(TwoPcVoteRequestMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} TwoPcVoteRequestMessage: {}", id, message.coordinatorId, message);

        final boolean canCommit;

        // Check client decision
        switch (message.decision) {
            case COMMIT: {
                // Check if transaction can commit
                canCommit = canCommit(message.transactionId);
                LOGGER.debug("DataStore {} received COMMIT from Coordinator {} and decision is {}", id, message.coordinatorId, TwoPcDecision.valueOf(canCommit));
                break;
            }
            case ABORT: {
                canCommit = false;
                LOGGER.debug("DataStore {} received ABORT from Coordinator {}", id, message.transactionId);
                break;
            }
            default:
                canCommit = false;
                LOGGER.warn("DataStore {} received UNKNOWN decision from Coordinator {}", id, message.transactionId);
                break;
        }

        // Send response to Coordinator
        final TwoPcVoteResponseMessage outMessage = new TwoPcVoteResponseMessage(id, message.transactionId, TwoPcDecision.valueOf(canCommit));
        getSender().tell(outMessage, getSender());
        LOGGER.debug("DataStore {} send to Coordinator {} TwoPcVoteResponseMessage: {}", id, message.coordinatorId, outMessage);
    }

    /**
     * Callback for {@link TwoPcDecisionMessage message}.
     *
     * @param message Received message
     */
    private void onTwoPcDecisionMessage(TwoPcDecisionMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} TwoPcDecisionMessage: {}", id, message.coordinatorId, message);

        // If decision is to commit, let's commit
        if (message.decision == TwoPcDecision.COMMIT) {
            synchronized (storage) {
                // Commit
                storage.putAll(workspaces.get(message.transactionId));
            }
            LOGGER.info("DataStore {} successfully committed transaction {}", id, message.transactionId);
        }

        // Clean resources
        cleanResources(message.transactionId);
    }

    /**
     * Callback for {@link SnapshotTokenMessage message}.
     *
     * @param message Received message
     */
    private void onSnapshotTokenMessage(SnapshotTokenMessage message) {
        LOGGER.debug("DataStore {} received from Coordinator {} SnapshotTokenMessage: {}", id, message.coordinatorId, message);
        LOGGER.trace("DataStore {} generating snapshot {} of storage: {}", id, message.snapshotId, storage);

        // Send response to Coordinator
        final SnapshotTokenResultMessage outMessage = new SnapshotTokenResultMessage(id, message.snapshotId, storage);
        getSender().tell(outMessage, getSelf());
        LOGGER.debug("DataStore {} send to Coordinator {} SnapshotTokenResultMessage: {}", id, message.coordinatorId, outMessage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStore that = (DataStore) o;
        return this.id == that.id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + (storage != null ? storage.hashCode() : 0);
        result = prime * result + (workspaces != null ? workspaces.hashCode() : 0);
        result = prime * result + (dataStores != null ? dataStores.hashCode() : 0);
        return result;
    }
}
