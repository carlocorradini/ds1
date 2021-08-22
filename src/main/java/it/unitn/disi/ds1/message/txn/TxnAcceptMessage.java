package it.unitn.disi.ds1.message.txn;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Reply message to {@link TxnBeginMessage} informing that the transaction
 * has been correctly accepted.
 */
public final class TxnAcceptMessage implements Serializable {
    private static final long serialVersionUID = -6339782978102970100L;

    /**
     * Construct a new TxnAcceptMessage class.
     */
    public TxnAcceptMessage() {}

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
