package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply from the coordinator receiving TxnBeginMsg.
 */
public final class TxnAcceptMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = -6339782978102970100L;

    public TxnAcceptMsg(UUID transactionId) {
        super(transactionId);
    }
}
