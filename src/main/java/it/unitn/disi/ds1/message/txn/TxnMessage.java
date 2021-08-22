package it.unitn.disi.ds1.message.txn;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * General abstract transaction message.
 */
public abstract class TxnMessage implements Serializable {
    private static final long serialVersionUID = -794548318351688710L;

    /**
     * Transaction id.
     */
    @Expose
    public final UUID transactionId;

    /**
     * Construct a new TxnMessage class.
     *
     * @param transactionId Transaction id
     */
    public TxnMessage(UUID transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
