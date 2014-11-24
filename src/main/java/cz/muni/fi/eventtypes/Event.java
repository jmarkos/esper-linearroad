package cz.muni.fi.eventtypes;

// implemented by all initial events created by DataDriver
public interface Event {

    public byte getType();
    public short getTime();

}
