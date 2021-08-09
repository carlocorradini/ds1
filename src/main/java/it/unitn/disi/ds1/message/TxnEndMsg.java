package it.unitn.disi.ds1.message;

import java.io.Serializable;

/**
 * Client sends to a coordinator to end the TXN.
 * It may ask for commit (with probability COMMIT_PROBABILITY), or abort.
 */
public final class TxnEndMsg implements Serializable {
    private static final long serialVersionUID = -7119663856673239183L;

    public final int clientId;
    public final boolean commit; // If false, the transaction should abort

    public TxnEndMsg(int clientId, boolean commit) {
        this.clientId = clientId;
        this.commit = commit;
    }
}
