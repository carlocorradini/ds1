package it.unitn.disi.ds1.message.twopc;

import it.unitn.disi.ds1.etc.Decision;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply message to {@link TwoPcVoteMessage}
 * from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * with the {@link Decision decision}.
 */
public final class TwoPcVoteResultMessage extends TwoPcMessage implements Serializable {
    private static final long serialVersionUID = 4917833122149828262L;

    /**
     * Construct a new TwoPcVoteResultMessage class.
     *
     * @param dataStoreId   DataStore id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcVoteResultMessage(int dataStoreId, UUID transactionId, Decision decision) {
        super(dataStoreId, transactionId, decision);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
