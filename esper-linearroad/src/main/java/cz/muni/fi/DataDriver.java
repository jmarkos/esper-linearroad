package cz.muni.fi;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;

import javax.swing.text.Position;

import cz.muni.fi.eventtypes.*;

public class DataDriver {

    public DataDriver(String fileLocation) {
        Path path = Paths.get(fileLocation);
        try {
            inputReader = Files.newBufferedReader(path, StandardCharsets.US_ASCII);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open the input file for data driver. ", e);
        }
    }

    private BufferedReader inputReader;
    private long simulationStart = 0;
    public long lastSecond = -1;
    private Event leftOverEvent = null;
    public boolean simulationEnded = false;
    private int speedup = 1; // to speed up the simulation (10 means 10800 / 10 = 1080 seconds)

    public void setSpeedup(int speedup) {
        this.speedup = speedup;
    }

    /*
     * Starts the simulation
     */
    public void start() {
        simulationStart = System.currentTimeMillis();
    }

    /*
     * Returns a collection of new events which happened after the previous
     * call of this method according to the elapsed simulation time.
     * Returns null if no new events happened.
     */
    public ArrayDeque<Event> getNewEvents() throws IOException {
        long currentTime = System.currentTimeMillis();
        long currentSecond = ((currentTime - simulationStart) / 1000) * speedup;
        ArrayDeque<Event> result = new ArrayDeque<Event>();
        if (currentSecond == lastSecond) {
            // no new events
            return null;
        }
//        System.out.println("currentSecond = " + currentSecond);
        lastSecond = currentSecond;
        String line = null;
        if (leftOverEvent != null) {
            result.add(leftOverEvent);
        }
        while ((line = inputReader.readLine()) != null) {
            Event e = parseLineToEvent(line);
            if (e == null) {
                continue;
            }
            if (e.getTime() <= currentSecond) {
                result.add(e);
            } else {
                // we're in the future, stop now but remember the first event from next second
                leftOverEvent = e;
                return result;
            }
        }
        simulationEnded = true;
        return result;
    }

    /*
        We're differentiating the events here to avoid creating a generic event and then throwing it away in Benchmark
     */
    public static Event parseLineToEvent(String line) {
        String[] data = line.split(",");
        byte type = Byte.valueOf(data[0]);
        switch (type) {
            case 0: // position report
                return new PositionReportEvent(data);
            case 2:
                return new AccountBalanceQuery(data);
            case 3:
                return new DailyExpenditureQuery(data);
            case 4:
                // Travel time estimation queries are not supported
                return null;
            default:
                System.out.println("Something is pretty wrong (unknown event type): " + Arrays.toString(data));
                return null; // just ignore the faulty line
        }
    }

    public static void simpleIterate() throws InterruptedException {
//        Path path = Paths.get("esper-lrb/data/datafile20seconds.dat");
        Path path = Paths.get("/home/van/dipl/parallel-esper/esper-lrb/data/datafile3hours.dat"); // 12mil lines
        long start = System.currentTimeMillis();
        ArrayDeque<Event> allevents = new ArrayDeque<Event>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.US_ASCII)){
            String line;
            int max = 0;
            while ((line = reader.readLine()) != null) {
                //process each line in some way
                Event e = parseLineToEvent(line);
                if (e != null) {
                    allevents.add(e);
                    if (e.getTime() > max) {
                        max = e.getTime();
                        if (max % 100 == 0 && max != 0)
                            System.out.println(max);
                    }
                    if (e instanceof PositionReportEvent) {
                        PositionReportEvent rpe = (PositionReportEvent) e;
                        if (rpe.speed == 0) {
                            System.out.println(rpe);
                        }
                    }
                }
//                int i = Integer.valueOf(line.split(",")[1]);
//                if (i > max) {
//                    max = i;
//                    if (max % 100 == 0 && max != 0)
//                        System.out.println(max);
//                }
            }
        } catch (IOException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
        long end = System.currentTimeMillis() - start;
        System.out.println("end = " + end);
        // 1.3GB
        System.out.println("allevents.size() = " + allevents.size());
        Thread.sleep(10000);
    }

    public static void main(String[] args) throws InterruptedException, IOException {
//        DataDriver dd = new DataDriver("esper-lrb/data/datafile20seconds.dat");
        DataDriver dd = new DataDriver("esper-lrb/data/datafile3hours.dat");
        dd.start();
        Thread.sleep(1900);

        int sum = 0;
        while (dd.simulationEnded == false) {
            ArrayDeque<Event> newEvents = dd.getNewEvents();
            if (newEvents != null) {
                sum += newEvents.size();
                System.out.println("sum = " + sum);
//                for (LRBEvent lrbEvent : newEvents) {
//                    System.out.println("lrbEvent = " + lrbEvent);
//                }
            }
            Thread.sleep(100);
        }
        System.out.println("sum = " + sum);
    }

}