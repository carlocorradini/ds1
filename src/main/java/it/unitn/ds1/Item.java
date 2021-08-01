package it.unitn.ds1;

public class Item {
    private int version;
    private int data;

    public Item() {
        this.version = 0;
        this.data = 100;
    } 

    public Item(int version, int data) {
        this.version = version;
        this.data = data;
    }

    public int getVersion() {
        return this.version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getData() {
        return this.data;
    }

    public void setData(int data) {
        this.data = data;
    }

    public String toString() {
        return "[Version: " + getVersion() + ", Data: " + getData() + "]";
    }
}
