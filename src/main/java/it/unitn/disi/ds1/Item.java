package it.unitn.disi.ds1;

public final class Item {
    public static final int DEFAULT_VERSION = 0;
    public static final int DEFAULT_DATA = 100;

    public final int version;
    public final int data;

    public Item(int version, int data) {
        this.version = version;
        this.data = data;
    }

    public Item() {
        this(DEFAULT_VERSION, DEFAULT_DATA);
    }

    @Override
    public String toString() {
        return String.format("[version: %d, data: %s]", version, data);
    }
}
