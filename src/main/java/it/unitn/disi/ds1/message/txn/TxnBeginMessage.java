package it.unitn.disi.ds1.message.txn;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Transaction begin message.
 */
public final class TxnBeginMessage implements Serializable {
    private static final long serialVersionUID = 7964732199270077332L;

    /**
     * Client id.
     */
    @Expose
    public final int clientId;

    /**
     * Construct a new TxnBeginMessage class.
     *
     * @param clientId Client id
     */
    public TxnBeginMessage(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
