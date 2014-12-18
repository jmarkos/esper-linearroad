package cz.muni.fi;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import cz.muni.fi.eventtypes.DailyExpenditureQuery;
import cz.muni.fi.eventtypes.DailyExpenditureResponse;
import org.apache.log4j.Logger;

/**
 * Connect to postgres:
 *     psql -U lrb -h localhost lrb
 * To preload data directly from file (instead of millions of inserts), use:
 *     CREATE UNLOGGED TABLE histtolls(vid int, day int, xway int, tolls int);
 *     ALTER TABLE ONLY histtolls ADD CONSTRAINT histtolls_primary PRIMARY KEY (vid, day);
 *     \copy <table name> FROM '<file path>' DELIMITER ',';
 *     (also comment the call to preloadData() function)
 * 660MB file -> 3GB table
 *
 * Other possible approaches - keeping the data only in memory, using espers tables (new in esper 5.1.0)
 */
public class DailyExpenditureProcessor {

    private static org.apache.log4j.Logger log = Logger.getLogger(DailyExpenditureProcessor.class);

    String user = "lrb";
    String password = "lrb";

    final ComboPooledDataSource ds; // c3p0 pooled datasource
    private BufferedReader inputReader;
    final ExecutorService executor; // thread pool to handle the incoming queries without blocking
    final OutputWriter outputWriter;

    // if histtollsfile is null, the historical tolls are already assumed to be in the DB
    public DailyExpenditureProcessor(String histtollsfile, OutputWriter outputWriter, String dbUrl) {
        this.outputWriter = outputWriter;
        if (histtollsfile != null) {
            Path path = Paths.get(histtollsfile);
            try {
                inputReader = Files.newBufferedReader(path, StandardCharsets.US_ASCII);
            } catch (IOException e) {
                throw new RuntimeException("Reading file " + histtollsfile + " failed: ", e);
            }
            preloadData();
        }

        ds = new ComboPooledDataSource();
        try {
            ds.setDriverClass( "org.postgresql.Driver" ); //loads the jdbc driver
        } catch (PropertyVetoException e) {
            throw new RuntimeException("No driver class was found? ", e);
        }
        ds.setJdbcUrl(dbUrl);
        ds.setUser(user);
        ds.setPassword(password);

        ds.setMinPoolSize(5);
        ds.setAcquireIncrement(5);
        ds.setMaxPoolSize(20);

        executor = Executors.newFixedThreadPool(20);
    }

    public void close() {
        ds.close();
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for executor termination. ", e);
        }
    }

    // little benchmark
    public static void main(String[] args) throws InterruptedException {
        DailyExpenditureProcessor dep = new DailyExpenditureProcessor(null, null, "jdbc:postgresql://localhost/lrb");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            dep.handleQuery(new DailyExpenditureQuery((byte) 0, (short) 10, (i % 50) + 1, (byte) 0, 999, (byte) ((i % 67) + 1)));
        }
        dep.executor.shutdown();
        dep.executor.awaitTermination(200, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - start;
        System.out.println("duration = " + duration);
    }

    public void handleQuery(final DailyExpenditureQuery deq) {
        if (deq.dayy == 0) { // there are no values in the historical-tolls file for day 0
            log.debug("Skipping DailyExpenditureQuery for day 0: " + deq);
            return;
        }
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Connection conn = null;
                try {
                    conn = ds.getConnection();
                    ResultSet rs = conn.createStatement().executeQuery("SELECT tolls FROM histtolls WHERE " +
                                                                                        "vid=" + deq.getVid() +
                                                                                        " and day=" + deq.getDayy() +
                                                                                        " and xway=" + deq.getXway());
                    rs.next();
                    DailyExpenditureResponse dailyExpenditureResponse = new DailyExpenditureResponse(deq.time, deq.qid, rs.getInt("tolls"));
                    outputWriter.outputDailyExpenditureResponse(dailyExpenditureResponse);
                } catch (SQLException e) {
                    log.error("Query " + deq + " failed with: ", e);
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            log.warn("Closing connection failed: ", e);
                        }
                    }
                }
            }
        });
    }

    public void preloadData() {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

        try {
            con = ds.getConnection();
            st = con.createStatement();
            int ret = 0;
            ret = st.executeUpdate("DROP TABLE IF EXISTS histtolls;");
            ret = st.executeUpdate("CREATE UNLOGGED TABLE histtolls(vid int, day int, xway int, tolls int);");
            ret = st.executeUpdate("ALTER TABLE ONLY histtolls ADD CONSTRAINT histtolls_primary PRIMARY KEY (vid, day);");

            PreparedStatement prepSt = con.prepareStatement("INSERT INTO histtolls(vid, day, xway, tolls) VALUES(?, ?, ?, ?)");

            String line = null;
            int count = 0;
            long start = System.currentTimeMillis();
            try {
                while ((line = inputReader.readLine()) != null) {
                    String[] data = line.split(" ");
                    int vid = Integer.valueOf(data[0]);
                    int day = Integer.valueOf(data[1]);
                    int xway = Integer.valueOf(data[2]);
                    int tolls = Integer.valueOf(data[3]);
                    prepSt.setInt(1, vid);
                    prepSt.setInt(2, day);
                    prepSt.setInt(3, xway);
                    prepSt.setInt(4, tolls);
                    prepSt.executeUpdate();
                    count++;
                    if (count % 100000 == 0) {
                        System.out.println("Preloaded... " + count);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Preloading data for daily expenditures failed: ", e);
            }
            long duration = System.currentTimeMillis() - start;
            System.out.println("Preloaded data (" + count + " rows) for daily expenditure queries in " + duration);
        } catch (SQLException e) {
            throw new RuntimeException("Preloading data for daily expenditures failed: ", e);
        } finally {
            try {if (rs != null) {rs.close();}if (st != null) {st.close();}if (con != null) {con.close();}
            } catch (SQLException e) {
                throw new RuntimeException("Preloading data for daily expenditures failed: ", e);
            }
        }
    }

}
