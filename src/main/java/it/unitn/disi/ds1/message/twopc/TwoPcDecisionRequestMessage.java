package it.unitn.disi.ds1.message.twopc;

import it.unitn.disi.ds1.message.txn.TxnMessage;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message
 * from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * to know the decision taken for a transaction.
 */
public final class TwoPcDecisionRequestMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = 1602544165609190264L;

    /**
     * Construct a new TxnMessage class.
     *
     * @param dataStoreId   {@link it.unitn.disi.ds1.actor.DataStore} id
     * @param transactionId {@link UUID Transaction} id
     */
    public TwoPcDecisionRequestMessage(int dataStoreId, UUID transactionId) {
        super(dataStoreId, transactionId);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
