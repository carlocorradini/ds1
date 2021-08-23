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

    /**
     * Sleep timeout (ms).
     */
    public static final int SLEEP_TIMEOUT_MS = 1000;

    /**
     * 2PC {@link it.unitn.disi.ds1.actor.Actor} recovery timeout (ms) after a crash.
     */
    public final static int TWOPC_RECOVERY_TIMEOUT_MS = 4000;
}
