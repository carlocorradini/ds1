package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from Coordinator to Servers to ask for committing or not
 */

public final class RequestMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 6797846417399441318L;

    public final Boolean doCommit;

    public RequestMsg(UUID transactionId, Boolean doCommit) {
        super(transactionId);
        this.doCommit = doCommit;
    }
}
