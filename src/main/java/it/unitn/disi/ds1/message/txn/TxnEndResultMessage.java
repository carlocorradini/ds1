package it.unitn.disi.ds1.message.txn;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.Client}
 * with outcome of transaction.
 */
public final class TxnEndResultMessage implements Serializable {
    private static final long serialVersionUID = -8747449002189796637L;

    /**
     * Decision made.
     */
    @Expose
    public final TwoPcDecision decision;

    /**
     * Construct a new TxnEndResultMessage class.
     *
     * @param decision Decision
     */
    public TxnEndResultMessage(TwoPcDecision decision) {
        this.decision = decision;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
