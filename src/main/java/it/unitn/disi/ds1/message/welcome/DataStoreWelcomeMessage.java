package it.unitn.disi.ds1.message.welcome;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.util.JsonUtil;
import it.unitn.disi.ds1.etc.ActorMetadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * {@link it.unitn.disi.ds1.actor.DataStore DataStore(s)} welcome message.
 */
public final class DataStoreWelcomeMessage implements Serializable {
    private static final long serialVersionUID = 8209158589637448646L;

    /**
     * Available {@link it.unitn.disi.ds1.actor.DataStore DataStore(s)}.
     */
    @Expose
    public final List<ActorMetadata> dataStores;

    /**
     * Construct a new DataStoreWelcomeMessage class.
     *
     * @param dataStores {@link it.unitn.disi.ds1.actor.DataStore DataStore(s)} metadata.
     */
    public DataStoreWelcomeMessage(List<ActorMetadata> dataStores) {
        this.dataStores = List.copyOf(dataStores);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
