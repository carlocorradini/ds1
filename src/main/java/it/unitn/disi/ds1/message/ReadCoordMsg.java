package it.unitn.disi.ds1.message;

import java.util.UUID;

import java.io.Serializable;

/**
 * READ request from the coordinator to the server.
 */
public final class ReadCoordMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 4166460770428474735L;

    // Item key
    public final int key;

    public ReadCoordMsg(UUID transactionId, int key) {
        super(transactionId);
        this.key = key;
    }
}
