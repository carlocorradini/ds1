package it.unitn.disi.ds1.actor;

import akka.actor.Props;
import it.unitn.disi.ds1.Item;
import it.unitn.disi.ds1.WriteRequest;
import it.unitn.disi.ds1.message.*;
import it.unitn.disi.ds1.message.ops.read.ReadCoordinatorMessage;
import it.unitn.disi.ds1.message.ops.read.ReadResultCoordinatorMessage;
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

    private final HashMap<Integer, Item> dataStore;
    private final List<WriteRequest> workspace; //Better data structure Hashmap<UUID, ArrayList<WriteRequest>>?

    // --- Constructors ---

    /**
     * Construct a new Data Store class.
     *
     * @param id Data Store id
     */
    public DataStore(int id) {
        super(id);
        LOGGER.debug("DataStore {} initialized", id);

        this.dataStore = new HashMap<>();
        this.workspace = new ArrayList<>();

        // Initialize items
        IntStream.range(id * 10, (id * 10) + 10).forEach(i -> dataStore.put(i, new Item()));
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
                .match(WriteCoordMsg.class, this::onWriteCoordMsg)
                .match(RequestMsg.class, this::onRequestMsg)
                .match(DecisionMsg.class, this::onDecisionMsg)
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
        final Item item = dataStore.get(key);

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

        final Item item = itemByKey(message.key);
        final ReadResultCoordinatorMessage outMessage = new ReadResultCoordinatorMessage(message.transactionId, message.key, item.value);
        getSender().tell(outMessage, getSelf());

        LOGGER.debug("DataStore {} send ReadResultCoordinatorMessage: {}", id, outMessage);
    }

    /*-- Actor methods -------------------------------------------------------- */
    private boolean checkVersion(UUID transactionId) {
        for (WriteRequest i : this.workspace) {
            if (i.transactionId.equals(transactionId) && i.actualVersion != this.dataStore.get(i.key).version) {
                return false;
            }
        }
        return true;
    }

    private void applyChanges(UUID transactionId) {
        for (WriteRequest i : this.workspace) {
            if (i.transactionId.equals(transactionId)) {
                this.dataStore.put(i.key, new Item(i.newValue, this.dataStore.get(i.key).version + 1));
            }
        }
    }

    /*-- Message handlers ----------------------------------------------------- */
    private void onWriteCoordMsg(WriteCoordMsg msg) {
        // Add write request to workspace
        this.workspace.add(new WriteRequest(msg.transactionId, msg.key, this.dataStore.get(msg.key).version, msg.value));
        // System.out.println(this.workspace);
    }

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
            System.out.printf("%s new values stored!\n%s\n", getSelf().path().name(), this.dataStore);
        }
        // Discard changes if Abort or Clear workspace
        this.workspace.removeIf(i -> i.transactionId.equals(msg.transactionId));
    }
}
