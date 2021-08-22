package it.unitn.disi.ds1.message.op.read;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;

/**
 * Reply message to {@link ReadCoordinatorMessage}
 * from {@link it.unitn.disi.ds1.actor.Coordinator} to {@link it.unitn.disi.ds1.actor.Client}
 * having the value of the corresponding key of the {@link Item}
 */
public final class ReadResultMessage implements Serializable {
    private static final long serialVersionUID = 6073342617515584698L;

    /**
     * {@link Item} key.
     */
    @Expose
    public final int key;

    /**
     * Item value.
     */
    @Expose
    public final int value;

    /**
     * Construct a new ReadResultMessage class.
     *
     * @param key           Item key
     * @param value         Item value
     */
    public ReadResultMessage(int key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
