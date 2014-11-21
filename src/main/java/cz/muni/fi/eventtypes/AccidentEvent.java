package cz.muni.fi.eventtypes;

public class AccidentEvent {

    public byte xway; // 0..L-1
    public byte direction; // 0 = East, 1 = West
    public byte segment; // 0..99, 1 mile long
    public int position;

    public AccidentEvent(byte xway, byte direction, byte segment, int position) {
        this.xway = xway;
        this.direction = direction;
        this.segment = segment;
        this.position = position;
    }

    @Override
    public String toString() {
        return "eventtypes.TrashedCarEvent{" +
                ", xway=" + xway +
                ", direction=" + direction +
                ", segment=" + segment +
                ", position=" + position +
                '}';
    }

    public byte getXway() {
        return xway;
    }

    public void setXway(byte xway) {
        this.xway = xway;
    }

    public byte getDirection() {
        return direction;
    }

    public void setDirection(byte direction) {
        this.direction = direction;
    }

    public byte getSegment() {
        return segment;
    }

    public void setSegment(byte segment) {
        this.segment = segment;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

}
