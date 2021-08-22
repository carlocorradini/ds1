package it.unitn.disi.ds1.message.txn;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.message.pc.two.TwoPcDecision;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Transaction end message.
 */
public final class TxnEndMessage extends Message implements Serializable {
    private static final long serialVersionUID = -7119663856673239183L;

    /**
     * Decision made.
     */
    @Expose
    public final TwoPcDecision decision;

    /**
     * Construct a new TxnEndMessage class.
     *
     * @param clientId Client id
     * @param decision Decision
     */
    public TxnEndMessage(int clientId, TwoPcDecision decision) {
        super(clientId);
        this.decision = decision;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
