package it.unitn.disi.ds1.message.txn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import it.unitn.disi.ds1.message.TxnMsg;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply message to {@link TxnBeginMessage} informing that the transaction
 * has been correctly accepted.
 */
public final class TxnAcceptMessage extends TxnMsg implements Serializable {
    private static final long serialVersionUID = -6339782978102970100L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Construct a new TxnAcceptMessage class.
     *
     * @param transactionId Transaction id
     */
    public TxnAcceptMessage(UUID transactionId) {
        super(transactionId);
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
