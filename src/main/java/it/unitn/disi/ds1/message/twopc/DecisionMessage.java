package it.unitn.disi.ds1.message.twopc;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.util.UUID;

/**
 * Message from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.DataStore}
 * to communicate Commit or Abort decision
 */

public final class DecisionMessage extends TwoPCMessage implements Serializable {
    private static final long serialVersionUID = 5152544683185426862L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();
    /**
     * Construct a new DecisionMessage class.
     *
     * @param transactionId Transaction id
     * @param  decision Decision
     */
    public DecisionMessage(UUID transactionId, boolean decision) {
        super(transactionId, decision);
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
