package it.unitn.disi.ds1.message.op.read;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.txn.TxnMessage;

import java.util.UUID;

import java.io.Serializable;

/**
 * Reply message to {@link ReadCoordinatorMessage}
 * from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * having the value of the corresponding key of the {@link Item}.
 */
public final class ReadResultCoordinatorMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = 2418188472950018347L;

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
     * {@link Item} key.
     */
    @Expose
    public final int key;

    /**
     * {@link Item} value.
     */
    @Expose
    public final int value;

    /**
     * Construct a new ReadResultCoordinatorMessage class.
     *
     * @param dataStoreId   DataStore id
     * @param transactionId Transaction id
     * @param key           Item key
     * @param value         Item value
     */
    public ReadResultCoordinatorMessage(int dataStoreId, UUID transactionId, int key, int value) {
        super(transactionId);
        this.dataStoreId = dataStoreId;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
