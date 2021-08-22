package it.unitn.disi.ds1.message.pc.two;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}
 * to communicate Commit or Abort decision
 */
public final class TwoPcDecisionMessage extends TwoPcMessage implements Serializable {
    private static final long serialVersionUID = 5152544683185426862L;

    /**
     * Construct a new DecisionMessage class.
     *
     * @param coordinatorId Coordinator id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcDecisionMessage(int coordinatorId, UUID transactionId, TwoPcDecision decision) {
        super(coordinatorId, transactionId, decision);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
