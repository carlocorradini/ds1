package it.unitn.disi.ds1.message.txn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;

/**
 * Reply message to {@link TxnBeginMessage} informing that the transaction
 * has been correctly accepted.
 */
public final class TxnAcceptMessage implements Serializable {
    private static final long serialVersionUID = -6339782978102970100L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Construct a new TxnAcceptMessage class.
     */
    public TxnAcceptMessage() {}

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
