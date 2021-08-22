package it.unitn.disi.ds1.message.pc.two;

import com.google.gson.annotations.Expose;
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
     * {@link it.unitn.disi.ds1.actor.DataStore} id.
     */
    @Expose
    public final int dataStoreId;

    /**
     * Construct a new TwoPcVoteResponseMessage class.
     *
     * @param dataStoreId   DataStore id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcVoteResponseMessage(int dataStoreId, UUID transactionId, TwoPcDecision decision) {
        super(transactionId, decision);
        this.dataStoreId = dataStoreId;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
