package it.unitn.disi.ds1.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from Coordinator to Servers to ask for committing or not
 */

public final class ResponseMsg extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 4917833122149828262L;

    public final Boolean yesOrNo;

    public ResponseMsg(UUID transactionId, Boolean yesOrNo) {
        super(transactionId);
        this.yesOrNo = yesOrNo;
    }
}
