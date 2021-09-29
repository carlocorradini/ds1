package it.unitn.disi.ds1.actor;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.txn.read.TxnReadMessage;
import it.unitn.disi.ds1.message.txn.read.TxnReadResultMessage;
import it.unitn.disi.ds1.message.txn.write.TxnWriteMessage;
import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.message.txn.*;
import it.unitn.disi.ds1.message.welcome.ClientWelcomeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.duration.Duration;

/**
 * Client {@link Actor actor} class.
 */
public final class Client extends AbstractActor {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Client.class);

    /**
     * Commit probability.
     */
    private static final double COMMIT_PROBABILITY = 1;

    /**
     * Write probability.
     */
    private static final double WRITE_PROBABILITY = 0.5;

    /**
     * Minimum transactions.
     */
    private static final int MIN_TXN_LENGTH = 8;

    /**
     * Maximum transactions.
     */
    private static final int MAX_TXN_LENGTH = 15;

    /**
     * Random range.
     */
    private static final int RAND_LENGTH_RANGE = MAX_TXN_LENGTH - MIN_TXN_LENGTH + 1;

    /**
     * Client identifier.
     */
    public final int id;

    /**
     * {@link Coordinator Coordinator(s)} metadata.
     */
    private final List<ActorMetadata> coordinators;

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
    private ActorMetadata txnCoordinator;

    /**
     * First {@link Item} key of the transaction.
     */
    private int txnFirstKey;

    /**
     * Second {@link Item} key of the transaction.
     */
    private int txnSecondKey;

    /**
     * First {@link Item} value of the transaction.
     */
    private Integer txnFirstValue;

    /**
     * Second {@link Item} value of the transaction.
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

    /**
     * {@link Random} instance.
     */
    private final Random random;

    /**
     * Run counter.
     */
    private int run;

    // --- Constructors ---

    /**
     * Construct a new Client class.
     *
     * @param id Client id
     */
    public Client(int id) {
        this.id = id;
        this.coordinators = new ArrayList<>();
        this.txnAttempted = 0;
        this.txnCommitted = 0;
        this.run = 0;
        // Initialize random with SecureRandom
        Random r;
        try {
            r = SecureRandom.getInstanceStrong();
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("Client {} Secure Random Number Generator (RNG) not found: {}. Fallback to standard Random", id, e.getMessage());
            r = new Random();
        }
        this.random = r;


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
                .match(TxnBeginResultMessage.class, this::onTxnBeginResultMessage)
                .match(TxnBeginTimeoutMessage.class, this::onTxnBeginTimeoutMessage)
                .match(TxnReadResultMessage.class, this::onTxnReadResultMessage)
                .match(TxnEndResultMessage.class, this::onTxnEndResultMsg)
                .match(TxnStopMessage.class, this::onTxnStopMessage)
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
            getContext().system().terminate();
        }

        txnAccepted = false;
        txnAttempted++;

        // Contact a random coordinator and begin a transaction
        final TxnBeginMessage outMessage = new TxnBeginMessage(id);
        txnCoordinator = coordinators.get(random.nextInt(coordinators.size()));
        txnCoordinator.ref.tell(outMessage, getSelf());
        LOGGER.debug("Client {} send to Coordinator {} TxnBeginMessage: {}", id, txnCoordinator.id, outMessage);

        // Total number of operations
        final int txtOpExtra = RAND_LENGTH_RANGE > 0 ? random.nextInt(RAND_LENGTH_RANGE) : 0;
        txnOpTotal = MIN_TXN_LENGTH + txtOpExtra;
        txnOpDone = 0;

        // Timeout confirmation of transaction by coordinator
        txnAcceptTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.create(500, TimeUnit.MILLISECONDS),
                getSelf(),
                new TxnBeginTimeoutMessage(),
                getContext().system().dispatcher(),
                getSelf()
        );

        LOGGER.info("Client {} BEGIN transaction", id);
    }

    /**
     * Read two {@link Item items}.
     */
    private void readTwo() {
        // Obtain items keys
        txnFirstKey = random.nextInt(maxItemKey + 1);
        final int randKeyOffset = 1 + random.nextInt(maxItemKey - 1);
        txnSecondKey = (txnFirstKey + randKeyOffset) % (maxItemKey + 1);

        // Read request 1
        final TxnReadMessage outFirstMessage = new TxnReadMessage(id, txnFirstKey);
        txnCoordinator.ref.tell(outFirstMessage, getSelf());
        LOGGER.debug("Client {} send to Coordinator {} TxnReadMessage: {}", id, txnCoordinator.id, outFirstMessage);

        // Read request 2
        final TxnReadMessage outSecondMessage = new TxnReadMessage(id, txnSecondKey);
        txnCoordinator.ref.tell(outSecondMessage, getSelf());
        LOGGER.debug("Client {} send to Coordinator {} TxnReadMessage: {}", id, txnCoordinator.id, outSecondMessage);

        // Delete the current read values
        txnFirstValue = null;
        txnSecondValue = null;

        LOGGER.info("Client {} READ #{} ({}), ({})", id, txnOpDone, txnFirstKey, txnSecondKey);
    }

    /**
     * Write two {@link Item items}.
     */
    private void writeTwo() {
        // Amount to add/remove
        final int amount = (txnFirstValue >= 1) ? 1 + random.nextInt(txnFirstValue) : 0;
        // New first value
        final int newFirstValue = txnFirstValue - amount;
        // New second value
        final int newSecondValue = txnSecondValue + amount;

        // Write request 1
        final TxnWriteMessage outFirstMessage = new TxnWriteMessage(id, txnFirstKey, newFirstValue);
        txnCoordinator.ref.tell(outFirstMessage, getSelf());
        LOGGER.debug("Client {} send to Coordinator {} TxnWriteMessage: {}", id, txnCoordinator.id, outFirstMessage);

        // Write request 2
        final TxnWriteMessage outSecondMessage = new TxnWriteMessage(id, txnSecondKey, newSecondValue);
        txnCoordinator.ref.tell(outSecondMessage, getSelf());
        LOGGER.debug("Client {} send to Coordinator {} TxnWriteMessage: {}", id, txnCoordinator.id, outSecondMessage);

        LOGGER.info("Client {} WRITE #{} taken {} ({}, {}), ({}, {})", id, txnOpDone, amount, txnFirstKey, newFirstValue, txnSecondKey, newSecondValue);
    }

    /**
     * End transaction.
     */
    private void endTxn() {
        final boolean commit = random.nextDouble() < COMMIT_PROBABILITY;
        final TxnEndMessage outMessage = new TxnEndMessage(id, Decision.valueOf(commit));

        txnCoordinator.ref.tell(outMessage, getSelf());
        txnFirstValue = null;
        txnSecondValue = null;

        LOGGER.debug("Client {} send to Coordinator {} TxnEndMessage: {}", id, txnCoordinator.id, outMessage);
        LOGGER.info("Client {} END transaction", id);
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
     * Callback for {@link TxnBeginResultMessage} message.
     *
     * @param message Received message
     */
    private void onTxnBeginResultMessage(TxnBeginResultMessage message) {
        LOGGER.debug("Client {} received TxnBeginResultMessage: {}", id, message);

        txnAccepted = true;
        txnAcceptTimeout.cancel();

        readTwo();
    }

    /**
     * Callback for {@link TxnBeginTimeoutMessage} message.
     *
     * @param message Received message
     */
    private void onTxnBeginTimeoutMessage(TxnBeginTimeoutMessage message) {
        LOGGER.warn("Client {} received TxnBeginTimeoutMessage: {}", id, message);

        if (!txnAccepted) {
            LOGGER.debug("Client {} start a new transaction due to timeout", id);
            beginTxn();
        } else {
            LOGGER.warn("Client {} ignoring timeout since there is a transaction running", id);
        }
    }

    /**
     * Callback for {@link TxnReadResultMessage} message.
     *
     * @param message Received message
     */
    private void onTxnReadResultMessage(TxnReadResultMessage message) {
        LOGGER.debug("Client {} received TxnReadResultMessage: {}", id, message);

        // Save read value(s)
        if (message.key == txnFirstKey) txnFirstValue = message.value;
        else if (message.key == txnSecondKey) txnSecondValue = message.value;

        final boolean opDone = (txnFirstValue != null && txnSecondValue != null);

        // Read or also write ?
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
     * Callback for {@link TxnEndResultMessage} message.
     *
     * @param message Received message
     */
    private void onTxnEndResultMsg(TxnEndResultMessage message) {
        LOGGER.debug("Client {} received TxnEndResultMessage with decision {}: {}", id, message.decision, message);

        // Increment run counter
        run += 1;

        switch (message.decision) {
            case COMMIT: {
                txnCommitted++;
                LOGGER.info("Client {} TXNs COMMIT OK ({}/{})", id, txnCommitted, txnAttempted);
                break;
            }
            case ABORT: {
                int txnFailed = txnAttempted - txnCommitted;
                LOGGER.info("Client {} TXNs COMMIT FAIL ({}/{})", id, txnFailed, txnAttempted);
                break;
            }
        }

        LOGGER.info("End TXN by Client {}", id);

        // If not in AUTOMATIC mode stop
        if (Config.MODE != Config.Mode.AUTOMATIC) return;
        // Check current run
        if (run < Config.N_RUNS) {
            // Begin a new transaction
            LOGGER.info("Client {} begin a new transaction in run {}/{}", id, run, Config.N_RUNS);
            beginTxn();
        } else {
            // Run ended
            LOGGER.info("Client {} ended all available {} run(s)", id, Config.N_RUNS);
        }
    }

    /**
     * Callback for {@link TxnStopMessage} message.
     *
     * @param message Received message
     */
    private void onTxnStopMessage(TxnStopMessage message) {
        getContext().stop(getSelf());
    }
}
