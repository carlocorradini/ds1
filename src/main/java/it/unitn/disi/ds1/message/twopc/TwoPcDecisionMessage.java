package it.unitn.disi.ds1.message.twopc;

import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}
 * to communicate the {@link it.unitn.disi.ds1.actor.Coordinator} final {@link Decision decision}.
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
    public TwoPcDecisionMessage(int coordinatorId, UUID transactionId, Decision decision) {
        super(coordinatorId, transactionId, decision);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
