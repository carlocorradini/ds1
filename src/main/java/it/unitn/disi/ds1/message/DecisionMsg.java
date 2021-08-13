package it.unitn.disi.ds1.message;

import it.unitn.disi.ds1.message.TxnMsg;

import java.io.Serializable;
import java.util.UUID;

public class DecisionMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 5152544683185426862L;

    public final boolean commitOrAbort;

    public DecisionMsg(UUID transactionId, boolean commitOrAbort) {
        super(transactionId);
        this.commitOrAbort = commitOrAbort;
    }
}
