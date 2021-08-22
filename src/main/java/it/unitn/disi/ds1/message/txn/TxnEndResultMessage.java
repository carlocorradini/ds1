package it.unitn.disi.ds1.message.txn;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.Client}
 * with outcome of transaction.
 */
public final class TxnEndResultMessage extends Message implements Serializable {
    private static final long serialVersionUID = -8747449002189796637L;

    /**
     * Decision made.
     */
    @Expose
    public final TwoPcDecision decision;

    /**
     * Construct a new TxnEndResultMessage class.
     *
     * @param coordinatorId {@link it.unitn.disi.ds1.actor.Coordinator} id
     * @param decision      Decision
     */
    public TxnEndResultMessage(int coordinatorId, TwoPcDecision decision) {
        super(coordinatorId);
        this.decision = decision;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
