package cz.muni.fi;

/**
 * Keeps track of simulation time.
 */
public class TimeService {

    long startTime;

    /**
     * @param startTime the time when the simulation will start, generated in LinearRoadTopology
     *                  when submitting the topology to storm
     */
    public TimeService(long startTime) {
        this.startTime = startTime;
    }

    /**
     * @return int the simulation current time (in seconds)
     */
    public int getTime() {
        return (int) ((System.currentTimeMillis() / 1000) - startTime);
    }

}
