package it.unitn.disi.ds1;

import it.unitn.disi.ds1.actor.Client;
import it.unitn.disi.ds1.actor.Coordinator;
import it.unitn.disi.ds1.actor.DataStore;

import java.io.Serializable;

/**
 * Configuration class.
 */
public final class Config implements Serializable {
    private static final long serialVersionUID = -4468675888064285514L;

    // --- Actor(s) ---

    /**
     * Number of {@link DataStore Data Store(s)}.
     */
    public final static int N_DATA_STORES = 4;

    /**
     * Number of {@link Coordinator Coordinator(s)}.
     */
    public final static int N_COORDINATORS = 8;

    /**
     * Number of {@link Client Client(s)}.
     */
    public final static int N_CLIENTS = 64;

    /**
     * Maximum item key index value.
     */
    public final static int MAX_ITEM_KEY = (N_DATA_STORES * 10) - 1;

    // --- Timeout ---

    /**
     * Minimum sleep timeout (ms).
     */
    public static final int MIN_SLEEP_TIMEOUT_MS = 16;

    /**
     * Maximum sleep timeout (ms).
     */
    public static final int MAX_SLEEP_TIMEOUT_MS = 512;

    /**
     * 2PC {@link Coordinator} vote to {@link DataStore DataStore(s)} timeout (ms).
     */
    public static final int TWOPC_VOTE_TIMEOUT_MS = 16000;

    /**
     * 2PC {@link DataStore} decision to {@link Coordinator} timeout (ms).
     */
    public static final int TWOPC_DECISION_TIMEOUT_MS = 16000;

    /**
     * 2PC {@link it.unitn.disi.ds1.actor.Actor} recovery timeout (ms) after a crash.
     */
    public final static int TWOPC_RECOVERY_TIMEOUT_MS = 320000;
}
