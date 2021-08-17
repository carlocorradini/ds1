package it.unitn.disi.ds1.actor;

import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import it.unitn.disi.ds1.message.*;
import it.unitn.disi.ds1.message.welcome.ClientWelcomeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.duration.Duration;

public final class Client extends Actor {
    private static final double COMMIT_PROBABILITY = 0.8;
    private static final double WRITE_PROBABILITY = 0.5;
    private static final int MIN_TXN_LENGTH = 2;
    private static final int MAX_TXN_LENGTH = 5;
    private static final int RAND_LENGTH_RANGE = MAX_TXN_LENGTH - MIN_TXN_LENGTH + 1;

    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Client.class);

    /**
     * List of available {@link Coordinator Coordinator(s)}.
     */
    private final List<ActorRef> coordinators;

    /**
     * Maximum key associated to items in {@link DataStore DataStore(s)F}.
     */
    private int maxItemKey;

    // Keep track of the number of TXNs (attempted, successfully committed)
    private int numAttemptedTxn;
    private int numCommittedTxn;

    // TXN operation (move some amount from a value to another)
    private boolean txnAccepted;
    private ActorRef txnCoordinator;
    private int firstKey, secondKey;
    private Integer firstValue, secondValue;
    private int numOpTotal;
    private int numOpDone;
    private Cancellable acceptTimeout;
    private Optional<UUID> txnId;

    private final Random random;

    public Client(int id) {
        super(id);
        this.coordinators = new ArrayList<>();
        LOGGER.debug("Client {} initialized", id);


        this.numAttemptedTxn = 0;
        this.numCommittedTxn = 0;
        this.txnId = Optional.empty();
        this.random = new Random();
    }

    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClientWelcomeMessage.class, this::onClientWelcomeMessage)
                .build();
    }

    // --- Methods ---

    // --- Message handlers ---
    private void onClientWelcomeMessage(ClientWelcomeMessage message) {
        LOGGER.debug("Client {} received welcome message: {}", id, message);

        // Coordinators
        coordinators.clear();
        coordinators.addAll(message.coordinators);
        // Max item key
        maxItemKey = message.maxItemKey;

        // Begin Transaction
        beginTxn();
    }

    /*-- Actor methods -------------------------------------------------------- */

    // start a new TXN: choose a random coordinator, send TxnBeginMsg and set timeout
    void beginTxn() {
        // some delay between transactions from the same client
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        txnAccepted = false;
        numAttemptedTxn++;

        // Contact a random coordinator and begin TXN
        txnCoordinator = coordinators.get(random.nextInt(coordinators.size()));
        txnCoordinator.tell(new TxnBeginMsg(id), getSelf());

        // how many operations (taking some amount and adding it somewhere else)?
        int numExtraOp = RAND_LENGTH_RANGE > 0 ? random.nextInt(RAND_LENGTH_RANGE) : 0;
        numOpTotal = MIN_TXN_LENGTH + numExtraOp;
        numOpDone = 0;

        // timeout for confirmation of TXN by the coordinator (sent to self)
        acceptTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.create(500, TimeUnit.MILLISECONDS),
                getSelf(),
                new TxnAcceptTimeoutMsg(), // message sent to myself
                getContext().system().dispatcher(),
                getSelf()
        );

        System.out.printf("%s BEGIN transaction%n", getSelf().path().name());
    }

    // end the current TXN sending TxnEndMsg to the coordinator
    void endTxn() {
        boolean doCommit = random.nextDouble() < COMMIT_PROBABILITY;
        txnCoordinator.tell(new TxnEndMsg(txnId.orElseThrow(NullPointerException::new), id, doCommit), getSelf());

        System.out.printf("%s END transaction %s%n", getSelf().path().name(), txnId.orElseThrow(NullPointerException::new));

        firstValue = null;
        secondValue = null;
        txnId = Optional.empty();
    }

    // READ two items (will move some amount from the value of the first to the second)
    void readTwo() {
        // read two different keys
        firstKey = random.nextInt(maxItemKey + 1);
        int randKeyOffset = 1 + random.nextInt(maxItemKey - 1);
        secondKey = (firstKey + randKeyOffset) % (maxItemKey + 1);

        // READ requests
        txnCoordinator.tell(new ReadMsg(txnId.orElseThrow(NullPointerException::new), id, firstKey), getSelf());
        txnCoordinator.tell(new ReadMsg(txnId.orElseThrow(NullPointerException::new), id, secondKey), getSelf());

        // delete the current read values
        firstValue = null;
        secondValue = null;

        System.out.printf("%s READ #%d (%d), (%d)%n",
                getSelf().path().name(), numOpDone, firstKey, secondKey);
    }

    // WRITE two items (called with probability WRITE_PROBABILITY after readTwo() values are returned)
    void writeTwo() {

        // take some amount from one value and pass it to the other, then request writes
        Integer amountTaken = 0;
        if (firstValue >= 1) amountTaken = 1 + random.nextInt(firstValue);
        txnCoordinator.tell(new WriteMsg(txnId.orElseThrow(NullPointerException::new), id, firstKey, firstValue - amountTaken), getSelf());
        txnCoordinator.tell(new WriteMsg(txnId.orElseThrow(NullPointerException::new), id, secondKey, secondValue + amountTaken), getSelf());

        System.out.printf("%s WRITE #%d taken %d (%d, %d), (%d, %d)%n",
                getSelf().path().name(), numOpDone, amountTaken, firstKey, (firstValue - amountTaken), secondKey, (secondValue + amountTaken));
    }

    /*-- Message handlers ----------------------------------------------------- */
    private void onStopMsg(StopMsg msg) {
        getContext().stop(getSelf());
    }

    private void onTxnAcceptMsg(TxnAcceptMsg msg) {
        txnId = Optional.of(msg.transactionId);
        txnAccepted = true;
        acceptTimeout.cancel();

        System.out.printf("Transaction accepted by %s with id %s%n", getSender().path().name(), txnId.orElseThrow(NullPointerException::new));
        readTwo();
    }

    private void onTxnAcceptTimeoutMsg(TxnAcceptTimeoutMsg msg) {
        if (!txnAccepted) beginTxn();
    }

    private void onReadResultMsg(ReadResultMsg msg) {
        System.out.printf("%s READ RESULT (%d, %d)%n", getSelf().path().name(), msg.key, msg.value);

        // Save read value(s)
        if (msg.key == firstKey) firstValue = msg.value;
        if (msg.key == secondKey) secondValue = msg.value;

        boolean opDone = (firstValue != null && secondValue != null);

        // do we only read or also write?
        double writeRandom = random.nextDouble();
        boolean doWrite = writeRandom < WRITE_PROBABILITY;
        if (doWrite && opDone) writeTwo();

        // check if the transaction should end;
        // otherwise, read two again
        if (opDone) numOpDone++;

        if (numOpDone >= numOpTotal) {
            endTxn();
        } else if (opDone) {
            readTwo();
        }
    }

    private void onTxnResultMsg(TxnResultMsg msg) {
        if (msg.commit) {
            numCommittedTxn++;
            System.out.printf("%s COMMIT OK (%d/%d)%n", getSelf().path().name(), numCommittedTxn, numAttemptedTxn);
        } else {
            System.out.printf("%s COMMIT FAIL (%d/%d)%n", getSelf().path().name(), numAttemptedTxn - numCommittedTxn, numAttemptedTxn);
        }

        System.out.printf("End TXN by %s\n", getSelf().path().name());
        ;

        //beginTxn();
    }
}
