package it.unitn.disi.ds1.message.op.write;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Write request message
 * from {@link it.unitn.disi.ds1.actor.Client} to {@link it.unitn.disi.ds1.actor.Coordinator}.
 */
public final class WriteMessage implements Serializable {
    private static final long serialVersionUID = 8248714506636891726L;

    /**
     * {@link it.unitn.disi.ds1.actor.Client} id
     */
    @Expose
    public final int clientId;

    /**
     * {@link Item} key to write.
     */
    @Expose
    public final int key;

    /**
     * New {@link Item} value to write.
     */
    @Expose
    public final int value;

    /**
     * Construct a new WriteMessage class.
     *
     * @param clientId      Client id
     * @param key           Item key to write
     * @param value         Item new value to write
     */
    public WriteMessage(int clientId, int key, int value) {
        this.clientId = clientId;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
