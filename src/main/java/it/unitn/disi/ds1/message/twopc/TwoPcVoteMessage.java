package it.unitn.disi.ds1.message.twopc;

import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}
 * asking if {@link it.unitn.disi.ds1.actor.DataStore} is able to COMMIT.
 */
public final class TwoPcVoteMessage extends TwoPcMessage implements Serializable {
    private static final long serialVersionUID = 6797846417399441318L;

    /**
     * Construct a new TwoPcVoteMessage class.
     *
     * @param coordinatorId Coordinator id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcVoteMessage(int coordinatorId, UUID transactionId, Decision decision) {
        super(coordinatorId, transactionId, decision);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
