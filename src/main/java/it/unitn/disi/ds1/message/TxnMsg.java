package it.unitn.disi.ds1.message;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.UUID;

/**
 * General abstract transaction message.
 */
public abstract class TxnMsg implements Serializable {
    private static final long serialVersionUID = -794548318351688710L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    @Expose
    public final UUID transactionId;

    public TxnMsg(UUID transactionId) {
        this.transactionId = transactionId;
    }
}
