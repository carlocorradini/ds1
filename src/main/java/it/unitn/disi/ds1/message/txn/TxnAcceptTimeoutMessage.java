package it.unitn.disi.ds1.message.txn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;

/**
 * Timeout message for {@link TxnBeginMessage} informing that no answer has been received.
 */
public final class TxnAcceptTimeoutMessage implements Serializable {
    private static final long serialVersionUID = 5632463252006918229L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Construct a new TxnAcceptTimeoutMessage class.
     */
    public TxnAcceptTimeoutMessage() {}

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
