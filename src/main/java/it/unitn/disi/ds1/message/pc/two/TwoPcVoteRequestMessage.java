package it.unitn.disi.ds1.message.pc.two;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}
 * asking if {@link it.unitn.disi.ds1.actor.DataStore} is able to commit or not
 */
public final class TwoPcVoteRequestMessage extends TwoPcMessage implements Serializable {
    private static final long serialVersionUID = 6797846417399441318L;

    /**
     * Construct a new TwoPcVoteRequestMessage class.
     *
     * @param coordinatorId Coordinator id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcVoteRequestMessage(int coordinatorId, UUID transactionId, TwoPcDecision decision) {
        super(coordinatorId, transactionId, decision);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
