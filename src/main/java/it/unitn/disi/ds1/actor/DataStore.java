package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import it.unitn.disi.ds1.Item;
import it.unitn.disi.ds1.message.ops.read.ReadCoordinatorMessage;
import it.unitn.disi.ds1.message.ops.read.ReadResultCoordinatorMessage;
import it.unitn.disi.ds1.message.ops.write.WriteCoordinatorMessage;
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
                /*.match(RequestMsg.class, this::onRequestMsg)
                .match(DecisionMsg.class, this::onDecisionMsg)*/
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
            // FIXME Try with something compatible with Akka
            System.exit(1);
        }

        return item;
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
        final Item oldItem = itemByKey(message.key);
        final Item newItem = new Item(message.value, oldItem.version);
        workspace.put(message.key, newItem);

        LOGGER.info("DataStore {} write request workspace in transaction {}: {}", id, message.transactionId, newItem);
    }

    /*-- Actor methods -------------------------------------------------------- */
    /*private boolean checkVersion(UUID transactionId) {
        for (WriteRequest i : this.workspace) {
            if (i.transactionId.equals(transactionId) && i.actualVersion != this.storage.get(i.key).version) {
                return false;
            }
        }
        return true;
    }

    private void applyChanges(UUID transactionId) {
        for (WriteRequest i : this.workspace) {
            if (i.transactionId.equals(transactionId)) {
                this.storage.put(i.key, new Item(i.newValue, this.storage.get(i.key).version + 1));
            }
        }
    }

    /*-- Message handlers ----------------------------------------------------- */
    /*
    private void onRequestMsg(RequestMsg msg) {
        if (msg.decision) {
            // Return to coordinator Yes or No
            System.out.printf("%s --> Reply to coordinator:%s\n", getSelf().path().name(), checkVersion(msg.transactionId));
            getSender().tell(new ResponseMsg(msg.transactionId, checkVersion(msg.transactionId)), getSender());
        } else {
            // Abort immediately
            System.out.printf("%s --> Client decides to ABORT immediately\n", getSelf().path().name());
            this.workspace.removeIf(i -> i.transactionId.equals(msg.transactionId));
        }
    }

    private void onDecisionMsg(DecisionMsg msg) {
        if (msg.decision) {
            // Apply changes if Commit
            applyChanges(msg.transactionId);
            System.out.printf("%s new values stored!\n%s\n", getSelf().path().name(), this.storage);
        }
        // Discard changes if Abort or Clear workspace
        this.workspace.removeIf(i -> i.transactionId.equals(msg.transactionId));
    }*/
}
