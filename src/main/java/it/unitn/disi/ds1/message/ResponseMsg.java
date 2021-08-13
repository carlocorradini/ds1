package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply from Servers to Coordinator with Yes or No decision
 */

public final class ResponseMsg extends TxnComMsg implements Serializable {
    private static final long serialVersionUID = 4917833122149828262L;

    public ResponseMsg(UUID transactionId, boolean decision) {
        super(transactionId, decision);
    }
}
