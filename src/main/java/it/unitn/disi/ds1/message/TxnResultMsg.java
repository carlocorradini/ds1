package it.unitn.disi.ds1.message;

import java.io.Serializable;

/**
 * Message from the coordinator to the client with the outcome of the TXN.
 */
public final class TxnResultMsg implements Serializable {
    private static final long serialVersionUID = -8747449002189796637L;

    public final Boolean commit; // If false, the transaction was aborted

    public TxnResultMsg(boolean commit) {
        this.commit = commit;
    }
}
