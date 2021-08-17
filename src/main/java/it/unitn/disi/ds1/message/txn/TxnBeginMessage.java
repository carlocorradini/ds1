package it.unitn.disi.ds1.message.txn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.io.Serializable;

/**
 * Transaction begin message.
 */
public final class TxnBeginMessage implements Serializable {
    private static final long serialVersionUID = 7964732199270077332L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Client id.
     */
    @Expose
    public final int clientId;

    /**
     * Construct a new TxnBeginMsg class.
     *
     * @param clientId Client id
     */
    public TxnBeginMessage(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
