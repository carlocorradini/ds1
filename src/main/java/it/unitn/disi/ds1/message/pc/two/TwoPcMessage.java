package it.unitn.disi.ds1.message.pc.two;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.txn.TxnMessage;

import java.io.Serializable;
import java.util.UUID;

/**
 * General abstract message for 2PC.
 */
public abstract class TwoPcMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = -7137752446196816824L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Decision made.
     */
    @Expose
    public final TwoPcDecision decision;

    /**
     * Construct a new TwoPCMessage class.
     *
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcMessage(UUID transactionId, TwoPcDecision decision) {
        super(transactionId);
        this.decision = decision;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
