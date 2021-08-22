package it.unitn.disi.ds1.message.txn;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Timeout message for {@link TxnBeginMessage} informing that no {@link TxnBeginResultMessage} has been received.
 */
public final class TxnBeginTimeoutMessage implements Serializable {
    private static final long serialVersionUID = 5632463252006918229L;

    /**
     * Construct a new TxnBeginTimeoutMessage class.
     */
    public TxnBeginTimeoutMessage() {
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
