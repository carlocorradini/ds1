package it.unitn.disi.ds1.message.txn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.io.Serializable;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.Client}
 * with outcome of transaction.
 */
public final class TxnResultMessage implements Serializable {
    private static final long serialVersionUID = -8747449002189796637L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();
    /**
     * Resulting decision.
     */
    @Expose
    public final Boolean commit;

    /**
     * Construct a new TxnResultMessage class.
     *
     * @param commit Txn outcome
     */
    public TxnResultMessage(boolean commit) {
        this.commit = commit;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
