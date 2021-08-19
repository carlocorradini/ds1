package it.unitn.disi.ds1.actor;

import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import it.unitn.disi.ds1.message.*;
import it.unitn.disi.ds1.message.ops.read.ReadMessage;
import it.unitn.disi.ds1.message.ops.read.ReadResultMessage;
import it.unitn.disi.ds1.message.ops.write.WriteMessage;
import it.unitn.disi.ds1.message.txn.*;
import it.unitn.disi.ds1.message.welcome.ClientWelcomeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.duration.Duration;

/**
 * Client {@link Actor actor} class.
 */
public final class Client extends Actor {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Client.class);

    /**
     * Commit probability.
     */
    private static final double COMMIT_PROBABILITY = 0.8;

    /**
     * Write probability.
     */
    private static final double WRITE_PROBABILITY = 0.5;

    /**
     * Minimum transactions.
     */
    private static final int MIN_TXN_LENGTH = 20;

    /**
     * Maximum transactions.
     */
    private static final int MAX_TXN_LENGTH = 40;

    /**
     * Random range.
     */
    private static final int RAND_LENGTH_RANGE = MAX_TXN_LENGTH - MIN_TXN_LENGTH + 1;

    /**
     * List of available {@link Coordinator Coordinator(s)}.
     */
    private final List<ActorRef> coordinators;

    /**
     * Maximum key associated to items in {@link DataStore DataStore(s)F}.
     */
    private int maxItemKey;

    // - Transactions
    /**
     * Attempted transactions.
     */
    private int txnAttempted;

    /**
     * Successfully committed transactions.
     */
    private int txnCommitted;

    /**
     * Accepted transaction.
     */
    private boolean txnAccepted;

    /**
     * {@link Coordinator} of the transaction.
     */
    private ActorRef txnCoordinator;

    /**
     * First {@link it.unitn.disi.ds1.Item} key of the transaction.
     */
    private int txnFirstKey;

    /**
     * Second {@link it.unitn.disi.ds1.Item} key of the transaction.
     */
    private int txnSecondKey;

    // FIXME I don't like Integer
    /**
     * First {@link it.unitn.disi.ds1.Item} value of the transaction.
     */
    private Integer txnFirstValue;

    /**
     * Second {@link it.unitn.disi.ds1.Item} value of the transaction.
     */
    private Integer txnSecondValue;

    /**
     * Total transactions operations.
     */
    private int txnOpTotal;

    /**
     * Successfully transactions operations.
     */
    private int txnOpDone;

    /**
     * Transaction accept {@link Cancellable timeout}.
     */
    private Cancellable txnAcceptTimeout;

    // FIXME Bleach
    /**
     * Transaction identifier.
     */
    private Optional<UUID> txnId;

    // --- Constructors ---

    /**
     * Construct a new Client class.
     *
     * @param id Client id
     */
    public Client(int id) {
        super(id);
        this.coordinators = new ArrayList<>();
        this.txnAttempted = 0;
        this.txnCommitted = 0;
        this.txnId = Optional.empty();
        LOGGER.debug("Client {} initialized", id);
    }

    /**
     * Return Client {@link Props}.
     *
     * @param id Client id
     * @return Client {@link Props}
     */
    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClientWelcomeMessage.class, this::onClientWelcomeMessage)
                .match(TxnAcceptMessage.class, this::onTxnAcceptMessage)
                .match(TxnAcceptTimeoutMessage.class, this::onTxnAcceptTimeoutMessage)
                .match(ReadResultMessage.class, this::onReadResultMessage)
                .match(TxnResultMessage.class,  this::onTxnResultMsg)
                .build();
    }

    // --- Methods ---

    /**
     * Start a new transaction.
     */
    private void beginTxn() {
        // Delay between transactions from the same client
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            LOGGER.error("{}", e.getMessage());
            e.printStackTrace();
            // FIXME Try with something compatible with Akka
            System.exit(1);
        }

        txnAccepted = false;
        txnAttempted++;

        // Contact a random coordinator and begin a transaction
        final TxnBeginMessage outMessage = new TxnBeginMessage(id);
        txnCoordinator = coordinators.get(random.nextInt(coordinators.size()));
        txnCoordinator.tell(outMessage, getSelf());
        LOGGER.debug("Client {} send TxnBeginMessage: {}", id, outMessage);

        // Total number of operations
        final int txtOpExtra = RAND_LENGTH_RANGE > 0 ? random.nextInt(RAND_LENGTH_RANGE) : 0;
        txnOpTotal = MIN_TXN_LENGTH + txtOpExtra;
        txnOpDone = 0;

        // Timeout confirmation of transaction by coordinator
        txnAcceptTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.create(500, TimeUnit.MILLISECONDS),
                getSelf(),
                new TxnAcceptTimeoutMessage(),
                getContext().system().dispatcher(),
                getSelf()
        );

        LOGGER.info("Client {} BEGIN transaction", id);
    }

    /**
     * End transaction.
     */
    private void endTxn() {
        final boolean commit = random.nextDouble() < COMMIT_PROBABILITY;
        final TxnEndMessage outMessage = new TxnEndMessage(txnId.orElseThrow(NullPointerException::new), id, commit);

        txnCoordinator.tell(outMessage, getSelf());
        txnFirstValue = null;
        txnSecondValue = null;
        txnId = Optional.empty();

        LOGGER.debug("Client {} send TxnEndMessage: {}", id, outMessage);
        LOGGER.info("Client {} END transaction", id);
    }

    /**
     * Read two {@link it.unitn.disi.ds1.Item items}.
     */
    private void readTwo() {
        // Obtain items keys
        txnFirstKey = random.nextInt(maxItemKey + 1);
        final int randKeyOffset = 1 + random.nextInt(maxItemKey - 1);
        txnSecondKey = (txnFirstKey + randKeyOffset) % (maxItemKey + 1);

        // Read request 1
        final ReadMessage outFirstMessage = new ReadMessage(txnId.orElseThrow(NullPointerException::new), id, txnFirstKey);
        txnCoordinator.tell(outFirstMessage, getSelf());
        LOGGER.debug("Client {} send ReadMessage: {}", id, outFirstMessage);

        // Read request 2
        final ReadMessage outSecondMessage = new ReadMessage(txnId.orElseThrow(NullPointerException::new), id, txnSecondKey);
        txnCoordinator.tell(outSecondMessage, getSelf());
        LOGGER.debug("Client {} send ReadMessage: {}", id, outSecondMessage);

        // Delete the current read values
        txnFirstValue = null;
        txnSecondValue = null;

        LOGGER.info("Client {} READ #{} ({}), ({})", id, txnOpDone, txnFirstKey, txnSecondKey);
    }

    /**
     * Write two {@link it.unitn.disi.ds1.Item items}.
     */
    private void writeTwo() {
        // Amount to add/remove
        final int amount = (txnFirstValue >= 1) ? 1 + random.nextInt(txnFirstValue) : 0;
        // New first value
        final int newFirstValue = txnFirstValue - amount;
        // New second value
        final int newSecondValue = txnSecondValue + amount;

        // Write request 1
        final WriteMessage outFirstMessage = new WriteMessage(txnId.orElseThrow(NullPointerException::new), id, txnFirstKey, newFirstValue);
        txnCoordinator.tell(outFirstMessage, getSelf());
        LOGGER.debug("Client {} send WriteMessage: {}", id, outFirstMessage);

        // Write request 2
        final WriteMessage outSecondMessage = new WriteMessage(txnId.orElseThrow(NullPointerException::new), id, txnSecondKey, newSecondValue);
        txnCoordinator.tell(outSecondMessage, getSelf());
        LOGGER.debug("Client {} send WriteMessage: {}", id, outSecondMessage);

        LOGGER.info("Client {} WRITE #{} taken {} ({}, {}), ({}, {})", id, txnOpDone, amount, txnFirstKey, newFirstValue, txnSecondKey, newSecondValue);
    }

    // --- Message handlers ---

    /**
     * Callback for {@link ClientWelcomeMessage} message.
     *
     * @param message Received message
     */
    private void onClientWelcomeMessage(ClientWelcomeMessage message) {
        LOGGER.debug("Client {} received ClientWelcomeMessage: {}", id, message);

        // Coordinators
        coordinators.clear();
        coordinators.addAll(message.coordinators);
        // Max item key
        maxItemKey = message.maxItemKey;

        // Begin Transaction
        beginTxn();
    }

    /**
     * Callback for {@link TxnAcceptMessage} message.
     *
     * @param message Received message
     */
    private void onTxnAcceptMessage(TxnAcceptMessage message) {
        LOGGER.debug("Client {} received TxnAcceptMessage: {}", id, message);

        txnId = Optional.of(message.transactionId);
        txnAccepted = true;
        txnAcceptTimeout.cancel();

        readTwo();
    }

    /**
     * Callback for {@link TxnAcceptTimeoutMessage} message.
     *
     * @param message Received message
     */
    private void onTxnAcceptTimeoutMessage(TxnAcceptTimeoutMessage message) {
        LOGGER.debug("Client {} received TxnAcceptTimeoutMessage", id);

        if (!txnAccepted) {
            LOGGER.debug("Client {} start a new transaction due to timeout", id);
            beginTxn();
        }
    }

    /**
     * Callback for {@link ReadResultMessage} message.
     *
     * @param message Received message
     */
    private void onReadResultMessage(ReadResultMessage message) {
        LOGGER.debug("Client {} received ReadResultMessage: {}", id, message);

        // Save read value(s)
        if (message.key == txnFirstKey) txnFirstValue = message.value;
        else if (message.key == txnSecondKey) txnSecondValue = message.value;

        final boolean opDone = (txnFirstValue != null && txnSecondValue != null);

        // Read or also write?
        final boolean doWrite = random.nextDouble() < WRITE_PROBABILITY;
        if (doWrite && opDone) writeTwo();

        // Check if transaction should end, otherwise read again
        if (opDone) txnOpDone++;
        if (txnOpDone >= txnOpTotal) {
            endTxn();
        } else if (opDone) {
            readTwo();
        }
    }

    /**
     * Callback for {@link TxnResultMessage} message.
     *
     * @param message Received message
     */
    private void onTxnResultMsg(TxnResultMessage message) {
        LOGGER.debug("Client {} received TxnResultMessage with decision commit/abort: {}", id, message.commit);

        if (message.commit) {
            txnCommitted++;
            LOGGER.info("Client {} TXNs COMMIT OK ({}/{})", id, txnCommitted, txnAttempted);
            System.out.printf("%s COMMIT OK (%d/%d)%n", getSelf().path().name(), txnCommitted, txnAttempted);
        } else {
            int txnFailed = txnAttempted - txnCommitted;
            LOGGER.info("Client {} TXNs COMMIT FAIL ({}/{})", id, txnFailed, txnAttempted);
        }

        LOGGER.info("End TXN by Client {}", id);

        //beginTxn();
    }

    /*-- Message handlers ----------------------------------------------------- */
    private void onStopMsg(StopMsg msg) {
        getContext().stop(getSelf());
    }

}
