package it.unitn.disi.ds1.message;

import java.io.Serializable;

/**
 * Client sends to a coordinator to begin the TXN.
 */
public final class TxnBeginMsg implements Serializable {
    private static final long serialVersionUID = 7964732199270077332L;

    public final Integer clientId;

    public TxnBeginMsg(int clientId) {
        this.clientId = clientId;
    }
}
