package it.unitn.disi.ds1.message.txn;

import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Transaction begin message
 * from {@link it.unitn.disi.ds1.actor.Client} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * asking to start a new {@link java.util.UUID transaction}.
 */
public final class TxnBeginMessage extends Message implements Serializable {
    private static final long serialVersionUID = 7964732199270077332L;

    /**
     * Construct a new TxnBeginMessage class.
     *
     * @param clientId Client id
     */
    public TxnBeginMessage(int clientId) {
        super(clientId);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
