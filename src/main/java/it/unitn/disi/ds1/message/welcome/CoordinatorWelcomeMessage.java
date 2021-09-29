package it.unitn.disi.ds1.message.welcome;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * {@link it.unitn.disi.ds1.actor.Coordinator Coordinator(s)} welcome message.
 */
public final class CoordinatorWelcomeMessage implements Serializable {
    private static final long serialVersionUID = -5587829195464438106L;

    /**
     * Available {@link it.unitn.disi.ds1.actor.DataStore DataStore(s)}.
     */
    @Expose
    public final List<ActorMetadata> dataStores;

    /**
     * Construct a new CoordinatorWelcomeMessage class.
     *
     * @param dataStores {@link it.unitn.disi.ds1.actor.DataStore DataStore(s)} metadata
     */
    public CoordinatorWelcomeMessage(List<ActorMetadata> dataStores) {
        this.dataStores = List.copyOf(dataStores);
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
