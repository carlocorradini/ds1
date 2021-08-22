package it.unitn.disi.ds1.message.txn.read;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.txn.TxnMessage;
import it.unitn.disi.ds1.util.JsonUtil;

import java.util.UUID;

import java.io.Serializable;

/**
 * Reply message to {@link TxnReadCoordinatorMessage}
 * from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * having the value of the corresponding key of the {@link Item}.
 */
public final class TxnReadResultCoordinatorMessage extends TxnMessage implements Serializable {
    private static final long serialVersionUID = 2418188472950018347L;

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
     * Construct a new TxnReadResultCoordinatorMessage class.
     *
     * @param dataStoreId   DataStore id
     * @param transactionId Transaction id
     * @param key           Item key
     * @param value         Item value
     */
    public TxnReadResultCoordinatorMessage(int dataStoreId, UUID transactionId, int key, int value) {
        super(dataStoreId, transactionId);
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
