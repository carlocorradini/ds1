package it.unitn.disi.ds1.message.twopc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.TxnMsg;

import java.io.Serializable;
import java.util.UUID;

/**
 * General abstract message for 2PC.
 */
public abstract class TwoPCMessage extends TxnMsg implements Serializable {
    private static final long serialVersionUID = -7137752446196816824L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Boolean variable used to communicate a decision.
     */
    @Expose
    public final boolean decision;

    /**
     * Construct a new TwoPCMessage class.
     *
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPCMessage(UUID transactionId, boolean decision) {
        super(transactionId);
        this.decision = decision;
    }
}
