package it.unitn.disi.ds1.message.txn;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * General message to stop a client.
 */
public final class TxnStopMessage implements Serializable {
    private static final long serialVersionUID = -574514237888465625L;

    /**
     * Construct a new TxnStopMessage class.
     */
    public TxnStopMessage() { }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
