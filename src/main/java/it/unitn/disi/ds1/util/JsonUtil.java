package it.unitn.disi.ds1.util;

import akka.actor.ActorRef;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import it.unitn.disi.ds1.util.adapter.serializer.ActorRefSerializer;

/**
 * Json utility class.
 */
public final class JsonUtil {
    /**
     * {@link Gson} instance.
     */
    public static transient final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .serializeNulls()
            .registerTypeAdapter(ActorRef.class, new ActorRefSerializer())
            .create();
}
