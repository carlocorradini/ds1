package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.op.read.ReadCoordinatorMessage;
import it.unitn.disi.ds1.message.op.read.ReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.op.write.WriteCoordinatorMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecisionMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcVoteRequestMessage;
import it.unitn.disi.ds1.message.pc.two.TwoPcVoteResponseMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
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
     * Storage used for persistency.
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
                .match(ReadCoordinatorMessage.class, this::onReadCoordinatorMessage)
                .match(WriteCoordinatorMessage.class, this::onWriteCoordinatorMessage)
                .match(TwoPcVoteRequestMessage.class, this::onTwoPcVoteRequestMessage)
                .match(TwoPcDecisionMessage.class, this::onTwoPcDecisionMessage)
                .build();
    }

    // --- Methods ---

    /**
     * Return {@link Item} by key.
     *
     * @param key Item key
     * @return Item
     */
    private Item itemByKey(int key) {
        final Item item = storage.get(key);

        if (item == null) {
            LOGGER.error("Unable to find Item by key {}", key);
            getContext().system().terminate();
        }

        return item;
    }

    /**
     * Return true if is able to commit changes made by {@link UUID transaction}, otherwise false.
     *
     * @param transactionId Transaction id
     * @return True if it can commit, false otherwise
     */
    private boolean canCommit(UUID transactionId) {
        // Obtain private workspace of the transaction
        final Map<Integer, Item> workspace = workspaces.get(transactionId);

        return workspace.entrySet().stream().allMatch((entry) -> {
            final Item itemInWorkSpace = entry.getValue();
            final Item itemInStorage = itemByKey(entry.getKey());

            return itemInWorkSpace.version == itemInStorage.version + 1;
        });
    }

    /**
     * Return true if is able to get locks on affected {@link Item} by {@link UUID transaction}, otherwise false.
     *
     * @param transactionId Transaction id
     * @return True if it gets locks, false otherwise
     */
    private boolean getLock(UUID transactionId) {
        // TODO: refactor with allMatch()
        // Obtain private workspace of the transaction
        final Map<Integer, Item> workspace = workspaces.get(transactionId);
        for (Map.Entry<Integer, Item> entry : workspace.entrySet()) {
            final Item itemInStorage = itemByKey(entry.getKey());
            if (itemInStorage.locked) {
                // Item already locked
                // Clean possible already set locks
                cleanLock(transactionId);
                return false;
            } else {
                // Lock item
                itemInStorage.locked = true;
            }
        }
        return true;
    }

    /**
     * Clean all possible locks on affected {@link Item} considering one transaction.
     *
     * @param transactionId Transaction id
     */
    private void cleanLock(UUID transactionId) {
        // Obtain private workspace of the transaction
        final Map<Integer, Item> workspace = workspaces.get(transactionId);
        workspace.forEach((key, value) -> itemByKey(key).locked = false);
    }

    // --- Message handlers ---

    /**
     * Callback for {@link ReadCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onReadCoordinatorMessage(ReadCoordinatorMessage message) {
        LOGGER.debug("DataStore {} received ReadCoordinatorMessage: {}", id, message);

        final Item item;

        // Obtain item for the current transaction
        final Map<Integer, Item> workspace = workspaces.get(message.transactionId);
        if (workspace != null && workspace.containsKey(message.key)) {
            // Item in workspace
            item = workspace.get(message.key);
            LOGGER.trace("DataStore {} ReadCoordinatorMessage item {} in transaction {} found in workspace: {}", id, message.key, message.transactionId, item);
        } else {
            // Item in storage
            item = itemByKey(message.key);
            LOGGER.trace("DataStore {} ReadCoordinatorMessage item {} in transaction {} found in storage: {}", id, message.key, message.transactionId, item);
        }

        final ReadResultCoordinatorMessage outMessage = new ReadResultCoordinatorMessage(message.transactionId, message.key, item.value);
        getSender().tell(outMessage, getSelf());

        LOGGER.debug("DataStore {} send ReadResultCoordinatorMessage: {}", id, outMessage);
    }

    /**
     * Callback for {@link WriteCoordinatorMessage} message.
     *
     * @param message Received message
     */
    private void onWriteCoordinatorMessage(WriteCoordinatorMessage message) {
        LOGGER.debug("DataStore {} received WriteCoordinatorMessage: {}", id, message);

        // Obtain private workspace, otherwise create
        final Map<Integer, Item> workspace = workspaces.computeIfAbsent(message.transactionId, k -> new HashMap<>());

        // Add new item to workspace
        final Item itemInStorage = itemByKey(message.key);
        final Item newItemInWorkspace = new Item(message.value, itemInStorage.version + 1, false);
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
        LOGGER.debug("DataStore {} received TwoPcVoteRequestMessage: {}", id, message);

        if (message.decision == TwoPcDecision.COMMIT) {
            // Inform Coordinator commit decision
            // TODO: try getting locks on storage
            final boolean canLock = getLock(message.transactionId);
            final boolean canCommit = canCommit(message.transactionId);
            final TwoPcVoteResponseMessage outMessage = new TwoPcVoteResponseMessage(id, message.transactionId, TwoPcDecision.valueOf(canCommit && canLock));
            getSender().tell(outMessage, getSender());
            LOGGER.debug("DataStore {} send TwoPcVoteResponseMessage: {}", id, outMessage);
        } else {
            // Delete workspace due to Client aborted transaction
            workspaces.remove(message.transactionId);
            LOGGER.debug("DataStore {} deleted workspace {} due to Client abort", id, message.transactionId);
        }
    }

    /**
     * Callback for {@link TwoPcDecisionMessage message}.
     *
     * @param message Received message
     */
    private void onTwoPcDecisionMessage(TwoPcDecisionMessage message) {
        // Add coordinator id
        LOGGER.debug("DataStore {} received TwoPcDecisionMessage: {}", id, message);

        if (message.decision == TwoPcDecision.COMMIT) {
            // Commit
            storage.putAll(workspaces.get(message.transactionId));
            LOGGER.info("DataStore {} successfully committed transaction {}", id, message.transactionId);
        }

        // Clean resources
        workspaces.remove(message.transactionId);
        LOGGER.trace("DataStore {} clean resources involving transaction {}", id, message.transactionId);

        //TODO: release locks on storage
    }
}
