package it.unitn.disi.ds1.message.welcome;

import akka.actor.ActorRef;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.adapter.serializer.ActorRefSerializer;
import it.unitn.disi.ds1.etc.ActorMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * {@link it.unitn.disi.ds1.actor.DataStore DataStore(s)} welcome message.
 */
public final class DataStoreWelcomeMessage implements Serializable {
    private static final long serialVersionUID = 8209158589637448646L;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .registerTypeAdapter(ActorRef.class, new ActorRefSerializer())
            .create();

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
        this.dataStores = dataStores;
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
