package it.unitn.disi.ds1.message.twopc;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message for {@link it.unitn.disi.ds1.actor.Coordinator} or {@link it.unitn.disi.ds1.actor.DataStore}
 * for communicating a timeout.
 */
public final class TwoPcTimeoutMessage implements Serializable {
    private static final long serialVersionUID = 3387272272729304297L;

    /**
     * Transaction id.
     */
    @Expose
    public final UUID transactionId;

    /**
     * Construct a new TxnMessage class.
     *
     * @param transactionId Transaction id
     */
    public TwoPcTimeoutMessage(UUID transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() { return JsonUtil.GSON.toJson(this); }
}
