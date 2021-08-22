package it.unitn.disi.ds1.message.txn.read;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.message.Message;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Read message
 * from {@link it.unitn.disi.ds1.actor.Client} to {@link it.unitn.disi.ds1.actor.Coordinator}.
 */
public final class TxnReadMessage extends Message implements Serializable {
    private static final long serialVersionUID = 278859909154339067L;

    /**
     * {@link Item} key to read.
     */
    @Expose
    public final int key;

    /**
     * Construct a new TxnReadMessage class.
     *
     * @param clientId Client id
     * @param key      Item key to read
     */
    public TxnReadMessage(int clientId, int key) {
        super(clientId);
        this.key = key;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
