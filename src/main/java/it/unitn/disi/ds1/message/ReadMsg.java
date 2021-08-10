package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * READ request from the client to the coordinator.
 */
public final class ReadMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 278859909154339067L;

    public final int clientId;
    public final int key; // Key of the value to read

    public ReadMsg(UUID transactionId, int clientId, int key) {
        super(transactionId);
        this.clientId = clientId;
        this.key = key;
    }
}
