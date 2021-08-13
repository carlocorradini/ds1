package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

public abstract class TxnComMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = -7137752446196816824L;
    
    public final boolean decision;

    public TxnComMsg(UUID transactionId, boolean decision) {
        super(transactionId);
        this.decision = decision;
    }
}
