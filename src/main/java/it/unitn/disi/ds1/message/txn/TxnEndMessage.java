package it.unitn.disi.ds1.message.txn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.TxnMessage;

import java.io.Serializable;
import java.util.UUID;

/**
 * Transaction end message.
 */
public final class TxnEndMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = -7119663856673239183L;

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
     * Commit or Abort decision taken by client.
     */
    @Expose
    public final boolean commit;

    /**
     * Construct a new TxnEndMessage class.
     *
     * @param transactionId Transaction id
     * @param clientId Client id
     * @param commit Commit or Abort decision
     */
    public TxnEndMessage(UUID transactionId, int clientId, boolean commit) {
        super(transactionId);
        this.clientId = clientId;
        this.commit = commit;
    }

    @Override
    public String toString() { return GSON.toJson(this); }
}