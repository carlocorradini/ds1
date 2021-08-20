package it.unitn.disi.ds1.etc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import it.unitn.disi.ds1.actor.DataStore;

/**
 * Simple item stored in a {@link DataStore}.
 */
public final class Item {
    /**
     * Default value.
     */
    public static final int DEFAULT_VALUE = 100;

    /**
     * Default version.
     */
    public static final int DEFAULT_VERSION = 0;

    public static final boolean DEFAULT_LOCKED = false;

    /**
     * Gson instance.
     */
    private static final Gson GSON = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    /**
     * Value of the item.
     * Any change applied updates the version (version + 1).
     */
    @Expose
    public final int value;

    /**
     * Version of the item.
     * Any change applied to value also updates its version (version + 1).
     * A new item has version set to DEFAULT_VERSION.
     */
    @Expose
    public final int version;

    @Expose
    public boolean locked;

    /**
     * Construct an item with the given value and version.
     *  @param value   Value of the item
     * @param version Version of the item
     * @param locked
     */
    public Item(int value, int version, boolean locked) {
        this.value = value;
        this.version = version;
        this.locked = locked;
    }

    /**
     * Construct an item with the given value and default version set to DEFAULT_VERSION.
     *
     * @param value Value of the item
     */
    public Item(int value) {
        this(value, DEFAULT_VERSION, DEFAULT_LOCKED);
    }

    /**
     * Construct an item with default value set to DEFAULT_VALUE and default version set to DEFAULT_VERSION.
     */
    public Item() {
        this(DEFAULT_VALUE, DEFAULT_VERSION, DEFAULT_LOCKED);
    }

    @Override
    public String toString() {
        return GSON.toJson(this);
    }
}
