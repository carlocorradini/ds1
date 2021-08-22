package it.unitn.disi.ds1.message.snap;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Reply message to {@link SnapshotTokenMessage}
 * from {@link it.unitn.disi.ds1.actor.DataStore} to {@link it.unitn.disi.ds1.actor.Coordinator}
 * containing the current storage.
 */
public final class SnapshotTokenResultMessage implements Serializable {
    private static final long serialVersionUID = 8848306550103300021L;

    /**
     * {@link it.unitn.disi.ds1.actor.DataStore} id.
     */
    @Expose
    public final int dataStoreId;

    /**
     * Snapshot id.
     */
    @Expose
    public final int snapshotId;

    /**
     * Data store storage.
     */
    @Expose
    public final Map<Integer, Item> storage;

    /**
     * Construct a new SnapshotTokenResultMessage class.
     *
     * @param dataStoreId {@link it.unitn.disi.ds1.actor.DataStore} id
     * @param snapshotId  Snapshot id
     * @param storage     Storage
     */
    public SnapshotTokenResultMessage(int dataStoreId, int snapshotId, Map<Integer, Item> storage) {
        this.dataStoreId = dataStoreId;
        this.snapshotId = snapshotId;
        this.storage = Collections.unmodifiableMap(storage);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
