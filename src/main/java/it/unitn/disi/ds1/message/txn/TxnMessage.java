package it.unitn.disi.ds1.message.txn;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * General abstract transaction message.
 */
public abstract class TxnMessage extends Message implements Serializable {
    private static final long serialVersionUID = -794548318351688710L;

    /**
     * Transaction id.
     */
    @Expose
    public final UUID transactionId;

    /**
     * Construct a new TxnMessage class.
     *
     * @param senderId      {@link it.unitn.disi.ds1.actor.Actor Sender} id
     * @param transactionId Transaction id
     */
    public TxnMessage(int senderId, UUID transactionId) {
        super(senderId);
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
