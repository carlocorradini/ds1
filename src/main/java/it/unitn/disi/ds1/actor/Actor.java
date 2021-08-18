package it.unitn.disi.ds1.actor;

import akka.actor.AbstractActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * General abstract actor.
 */
public abstract class Actor extends AbstractActor {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(Client.class);

    /**
     * Secure random instance.
     */
    protected final SecureRandom random;

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

        // Initialize secure random
        SecureRandom sr = null;
        try {
            sr = SecureRandom.getInstanceStrong();
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Secure Random Number Generator (RNG) not found: {}", e.getMessage());
            e.printStackTrace();
            // FIXME Try with something compatible with Akka
            System.exit(1);
        }
        this.random = sr;
    }
}
