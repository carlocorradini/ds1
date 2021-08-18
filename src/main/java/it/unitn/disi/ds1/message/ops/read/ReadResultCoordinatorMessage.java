package it.unitn.disi.ds1.message.ops.read;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.message.TxnMsg;

import java.util.UUID;

import java.io.Serializable;

/**
 * Reply message to {@link ReadCoordinatorMessage}
 * from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * having the value of the corresponding key of the {@link it.unitn.disi.ds1.Item}.
 */
public final class ReadResultCoordinatorMessage extends TxnMsg implements Serializable {
    private static final long serialVersionUID = 2418188472950018347L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * {@link it.unitn.disi.ds1.Item} key.
     */
    @Expose
    public final int key;

    /**
     * {@link it.unitn.disi.ds1.Item} value.
     */
    @Expose
    public final int value;

    /**
     * Construct a new ReadResultCoordinatorMessage class.
     *
     * @param transactionId Transaction id
     * @param key           Item key
     * @param value         Item value
     */
    public ReadResultCoordinatorMessage(UUID transactionId, int key, int value) {
        super(transactionId);
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}