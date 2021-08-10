package it.unitn.disi.ds1.message;

import java.util.UUID;

import java.io.Serializable;

/**
 * Reply from the server when requested a READ on a given key by the coordinator.
 */
public final class ReadResultCoordMsg implements Serializable {
    private static final long serialVersionUID = 2L;

    public final UUID transactionId;
    public final int key; // Key of the value to read
    public final int value; // Value found in the data store for that item

    public ReadResultCoordMsg(UUID transactionId, int key, int value) {
        this.transactionId = transactionId;
        this.key = key;
        this.value = value;
    }
}
