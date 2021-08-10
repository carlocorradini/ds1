package it.unitn.disi.ds1.message;

import java.util.UUID;

import java.io.Serializable;

/**
 * WRITE request from the coordinator to the server.
 */
public final class WriteCoordMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = -4823398098700891377L;

    public final int key;
    public final int value;

    public WriteCoordMsg(UUID transactionId, int key, int value) {
        super(transactionId);
        this.key = key;
        this.value = value;
    }
}
