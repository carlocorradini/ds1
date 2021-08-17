package it.unitn.disi.ds1.actor;

import akka.actor.AbstractActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
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

        // Try initialize random (secure)
        SecureRandom secureRandom = null;
        try {
            // Custom RNG
            secureRandom = SecureRandom.getInstance("SHA1PRNG", "SUN");
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            LOGGER.warn("Secure Random Number Generator (RNG) `SHA1PRNG:SUN` not found: {}", e.getMessage());
            try {
                // Default RNG
                secureRandom = SecureRandom.getInstanceStrong();
            } catch (NoSuchAlgorithmException eDefault) {
                LOGGER.error("Secure Random Number Generator (RNG) not found: {}", eDefault.getMessage());
                eDefault.printStackTrace();
                // FIXME Try with something compatible with Akka
                System.exit(1);
            }
        }
        this.random = secureRandom;
    }
}
