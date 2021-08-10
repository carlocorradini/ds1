package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * Client sends to a coordinator to end the transaction.
 * It may ask for commit (with probability COMMIT_PROBABILITY), or abort.
 */
public final class TxnEndMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = -7119663856673239183L;

    public final int clientId;
    // If false, the transaction should abort
    public final boolean commit;

    public TxnEndMsg(UUID transactionId, int clientId, boolean commit) {
        super(transactionId);
        this.clientId = clientId;
        this.commit = commit;
    }
}
