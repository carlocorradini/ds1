package it.unitn.disi.ds1.message.pc.two;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}
 * to communicate Commit or Abort decision
 */
public final class TwoPcDecisionMessage extends TwoPcMessage implements Serializable {
    private static final long serialVersionUID = 5152544683185426862L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * {@link it.unitn.disi.ds1.actor.Coordinator} id.
     */
    @Expose
    public final int coordinatorId;

    /**
     * Construct a new DecisionMessage class.
     *
     * @param coordinatorId Coordinator id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcDecisionMessage(int coordinatorId, UUID transactionId, TwoPcDecision decision) {
        super(transactionId, decision);
        this.coordinatorId = coordinatorId;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
