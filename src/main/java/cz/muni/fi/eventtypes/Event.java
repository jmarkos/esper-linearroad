package cz.muni.fi.eventtypes;
// simple marker interface for all possible input event types
public interface Event {

    public byte getType();
    public short getTime();

}
