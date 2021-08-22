package it.unitn.disi.ds1.message.op.read;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Read request message
 * from {@link it.unitn.disi.ds1.actor.Client} to {@link it.unitn.disi.ds1.actor.Coordinator}.
 */
public final class ReadMessage implements Serializable {
    private static final long serialVersionUID = 278859909154339067L;

    /**
     * {@link it.unitn.disi.ds1.actor.Client} id.
     */
    @Expose
    public final int clientId;

    /**
     * {@link Item} key to read.
     */
    @Expose
    public final int key;

    /**
     * Construct a new ReadMessage class.
     *
     * @param clientId      Client id
     * @param key           Item key to read
     */
    public ReadMessage(int clientId, int key) {
        this.clientId = clientId;
        this.key = key;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
