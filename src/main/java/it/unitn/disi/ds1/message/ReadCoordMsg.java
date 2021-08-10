package it.unitn.disi.ds1.message;

import java.util.UUID;

import java.io.Serializable;

/**
 * READ request from the coordinator to the server.
 */
public final class ReadCoordMsg implements Serializable {
    private static final long serialVersionUID = 1L;

    public final UUID transactionId;
    public final int key; // Key of the value to read

    public ReadCoordMsg(UUID transactionId, int key) {
        this.transactionId = transactionId;
        this.key = key;
    }
}
