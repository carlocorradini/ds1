package it.unitn.disi.ds1.message.pc.two;

import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * with Yes or No decision for committing
 */
public final class TwoPcVoteResponseMessage extends TwoPcMessage implements Serializable {
    private static final long serialVersionUID = 4917833122149828262L;

    /**
     * Construct a new TwoPcVoteResponseMessage class.
     *
     * @param dataStoreId   DataStore id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcVoteResponseMessage(int dataStoreId, UUID transactionId, TwoPcDecision decision) {
        super(dataStoreId, transactionId, decision);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
