package it.unitn.disi.ds1.message;

import java.util.UUID;

import java.io.Serializable;

/**
 * WRITE request from the coordinator to the server.
 */
public final class WriteCoordMsg implements Serializable {
    private static final long serialVersionUID = 3L;

    public final UUID transactionId;
    public final int key;
    public final int value;

    public ReadCoordMsg(UUID transactionId, int key, int value) {
        this.transactionId = transactionId;
        this.key = key;
        this.value = value;
    }
}
