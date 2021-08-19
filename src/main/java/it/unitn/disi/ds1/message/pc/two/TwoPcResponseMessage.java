package it.unitn.disi.ds1.message.pc.two;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * with Yes or No decision for committing
 */
public final class TwoPcResponseMessage extends TwoPcMessage implements Serializable {
    private static final long serialVersionUID = 4917833122149828262L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * {@link it.unitn.disi.ds1.actor.DataStore} id.
     */
    @Expose
    public final int dataStoreId;

    /**
     * Construct a new ResponseMessage class.
     *
     * @param dataStoreId   DataStore id
     * @param transactionId Transaction id
     * @param decision      Decision
     */
    public TwoPcResponseMessage(int dataStoreId, UUID transactionId, TwoPcDecision decision) {
        super(transactionId, decision);
        this.dataStoreId = dataStoreId;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
