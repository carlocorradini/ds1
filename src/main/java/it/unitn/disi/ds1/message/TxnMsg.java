package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * General abstract transaction message.
 */
public abstract class TxnMsg implements Serializable {
    private static final long serialVersionUID = -794548318351688710L;

    public final UUID transactionId;

    public TxnMsg(UUID transactionId) {
        this.transactionId = transactionId;
    }
}
