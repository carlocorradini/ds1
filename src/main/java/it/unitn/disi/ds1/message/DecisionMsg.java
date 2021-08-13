package it.unitn.disi.ds1.message;

import it.unitn.disi.ds1.message.TxnMsg;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from Coordinator to Servers to communicate Commit or Abort decision
 */

public final class DecisionMsg extends TxnComMsg implements Serializable {
    private static final long serialVersionUID = 5152544683185426862L;

    public DecisionMsg(UUID transactionId, boolean decision) {
        super(transactionId, decision);
    }
}
