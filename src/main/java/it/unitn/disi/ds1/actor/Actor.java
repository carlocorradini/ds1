package it.unitn.disi.ds1.actor;

import akka.actor.AbstractActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * General abstract actor.
 */
public abstract class Actor extends AbstractActor {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Client.class);

    /**
     * {@link Random} instance.
     */
    protected final Random random;

    /**
     * Actor identifier.
     */
    public final int id;

    /**
     * Construct a new Actor class.
     *
     * @param id Actor identifier
     */
    public Actor(int id) {
        this.id = id;

        // Initialize random with SecureRandom
        Random r;
        try {
            r = SecureRandom.getInstanceStrong();
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("Secure Random Number Generator (RNG) not found: {}. Fallback to standard Random", e.getMessage());
            r = new Random();
        }
        this.random = r;
    }
}
