package it.unitn.disi.ds1.actor;

import akka.actor.AbstractActor;
import it.unitn.disi.ds1.Config;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.message.twopc.TwoPcRecoveryMessage;
import it.unitn.disi.ds1.message.twopc.TwoPcTimeoutMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.duration.Duration;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * General abstract {@link AbstractActor actor}.
 */
public abstract class Actor extends AbstractActor {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Actor.class);

    /**
     * Actor identifier.
     */
    public final int id;

    /**
     * {@link Random} instance.
     */
    protected final Random random;

    /**
     * Final {@link Decision} of the Actor for a {@link UUID transaction}.
     */
    protected final Map<UUID, Decision> finalDecisions;

    /**
     * Construct a new Actor class.
     *
     * @param id Actor identifier
     */
    public Actor(int id) {
        this.id = id;
        this.finalDecisions = new HashMap<>();

        // Initialize random with SecureRandom
        Random r;
        try {
            r = SecureRandom.getInstanceStrong();
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("Actor {} Secure Random Number Generator (RNG) not found: {}. Fallback to standard Random", id, e.getMessage());
            r = new Random();
        }
        this.random = r;
    }

    /**
     * Send in multicast to the recipients the message.
     * If crash is true, simulate a crash during the multicast operation.
     *
     * @param recipients {@link Actor} recipients
     * @param message    Message to send
     * @param crash      Crash during multicast
     */
    protected void multicast(List<ActorMetadata> recipients, Message message, boolean crash) {
        if (recipients == null) return;

        for (ActorMetadata recipient : recipients) {
            LOGGER.debug("Actor {} send to multicast involving {} recipient(s): {}", id, recipients.size(), message);
            recipient.ref.tell(message, getSelf());

            if (crash) {
                crash();
                break;
            }
        }
    }

    /**
     * Send in multicast to the recipients the message.
     *
     * @param recipients {@link Actor} recipients
     * @param message    Message to send
     */
    protected void multicast(List<ActorMetadata> recipients, Message message) {
        multicast(recipients, message, false);
    }

    /**
     * Check if the Actor has already decided for the given {@link UUID transaction}.
     *
     * @param transactionId {@link UUID Transaction} id
     * @return True if already decided, false otherwise
     */
    protected boolean hasDecided(UUID transactionId) {
        return finalDecisions.containsKey(transactionId);
    }

    /**
     * Set the final {@link Decision decision} of the Actor for the given {@link UUID transaction}.
     *
     * @param transactionId {@link UUID Transaction} id
     * @param decision      Actor decision
     */
    protected void decide(UUID transactionId, Decision decision) {
        finalDecisions.computeIfAbsent(transactionId, k -> {
            LOGGER.debug("Actor {} has decided to {} for transaction {}", id, decision, transactionId);
            return decision;
        });
    }

    /**
     * Sleep.
     *
     * @param sleepTimeout Timeout in ms to sleep
     */
    protected void sleep(int sleepTimeout) {
        try {
            Thread.sleep(sleepTimeout);
        } catch (InterruptedException e) {
            LOGGER.error("Actor {} sleep error: {}", id, e.getMessage());
            e.printStackTrace();
            getContext().system().terminate();
        }
    }

    /**
     * Sleep.
     */
    protected void sleep() {
        sleep(random
                .ints(Config.MIN_SLEEP_TIMEOUT_MS, Config.MAX_SLEEP_TIMEOUT_MS + 1)
                .findFirst()
                .orElse(Math.abs(Config.MAX_SLEEP_TIMEOUT_MS - Config.MIN_SLEEP_TIMEOUT_MS)));
    }

    /**
     * Crashed state.
     *
     * @return Receive state
     */
    private Receive crashed() {
        return receiveBuilder()
                .match(TwoPcRecoveryMessage.class, this::onTwoPcRecoveryMessage)
                .matchAny(message -> LOGGER.warn("Actor {} on crashed received unmatched message: ", message))
                .build();
    }

    /**
     * Simulate Actor crash.
     *
     * @param recoveryTimeout Timeout in ms before Actor recovery
     */
    protected void crash(int recoveryTimeout) {
        // Become crashed
        getContext().become(crashed());
        LOGGER.info("Actor {} is crashed", id);

        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoveryTimeout, TimeUnit.MILLISECONDS),
                getSelf(),
                new TwoPcRecoveryMessage(),
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    /**
     * Simulate Actor crash.
     */
    protected void crash() {
        crash(Config.TWOPC_RECOVERY_TIMEOUT_MS);
    }

    /**
     * Simulate Actor timeout.
     *
     * @param timeout       Timeout in ms
     * @param transactionId Transaction id during timeout
     */
    protected void timeout(int timeout, UUID transactionId) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(timeout, TimeUnit.MILLISECONDS),
                getSelf(),
                new TwoPcTimeoutMessage(transactionId),
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    /**
     * Simulate Actor timeout.
     *
     * @param transactionId Transaction id during timeout
     */
    protected void timeout(UUID transactionId) {
        timeout(random
                .ints(Config.MIN_SLEEP_TIMEOUT_MS, Config.MAX_SLEEP_TIMEOUT_MS + 1)
                .findFirst()
                .orElse(Math.abs(Config.MAX_SLEEP_TIMEOUT_MS - Config.MIN_SLEEP_TIMEOUT_MS)), transactionId);
    }

    /**
     * Callback for {@link TwoPcRecoveryMessage} message.
     *
     * @param message Received message
     */
    protected abstract void onTwoPcRecoveryMessage(TwoPcRecoveryMessage message);

    /**
     * Callback for {@link TwoPcTimeoutMessage} message.
     *
     * @param message Received message
     */
    protected abstract void onTwoPcTimeoutMessage(TwoPcTimeoutMessage message);
}
