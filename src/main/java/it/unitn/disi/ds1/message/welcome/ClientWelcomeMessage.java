package it.unitn.disi.ds1.message.welcome;

import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.etc.ActorMetadata;
import it.unitn.disi.ds1.etc.Item;
import it.unitn.disi.ds1.util.JsonUtil;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * {@link it.unitn.disi.ds1.actor.Client Client(s)} welcome message.
 */
public final class ClientWelcomeMessage implements Serializable {
    private static final long serialVersionUID = -6994226840370853539L;

    /**
     * Available {@link it.unitn.disi.ds1.actor.Client Client(s)}.
     */
    @Expose
    public final List<ActorMetadata> coordinators;

    /**
     * Maximum {@link Item Item} key.
     */
    @Expose
    public final int maxItemKey;

    /**
     * Construct a new ClientWelcomeMessage class.
     *
     * @param coordinators {@link it.unitn.disi.ds1.actor.Client Client(s)} metadata
     * @param maxItemKey   Maximum {@link Item Item} key
     */
    public ClientWelcomeMessage(List<ActorMetadata> coordinators, int maxItemKey) {
        this.coordinators = List.copyOf(coordinators);
        this.maxItemKey = maxItemKey;
    }

    @Override
    public String toString() {
        return JsonUtil.GSON.toJson(this);
    }
}
