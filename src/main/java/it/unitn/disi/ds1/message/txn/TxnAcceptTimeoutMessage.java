package it.unitn.disi.ds1.message.txn;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Timeout message for {@link TxnBeginMessage} informing that no answer has been received.
 */
public final class TxnAcceptTimeoutMessage implements Serializable {
    private static final long serialVersionUID = 5632463252006918229L;

    /**
     * Construct a new TxnAcceptTimeoutMessage class.
     */
    public TxnAcceptTimeoutMessage() {}

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
