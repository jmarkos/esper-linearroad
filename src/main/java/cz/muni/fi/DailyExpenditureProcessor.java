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

/**
 * Connect to postgres:
 *     psql -U lrb -h localhost lrb
 * To preload data directly from file (instead of millions of inserts), use:
 *     CREATE UNLOGGED TABLE histtolls(vid int, day int, xway int, tolls int);
 *     ALTER TABLE ONLY histtolls ADD CONSTRAINT histtolls_primary PRIMARY KEY (vid, day);
 *     \copy <table name> FROM '<file path>' DELIMITER ' ';
 *     (also comment the call to preloadData() function)
 *
 * Other possible approaches - keeping the data only in memory, using espers tables (new in esper 5.1.0)
 */
public class DailyExpenditureProcessor {

    String url = "jdbc:postgresql://localhost/lrb";
    String user = "lrb";
    String password = "lrb";

    final ComboPooledDataSource ds; // c3p0 pooled datasource
    private BufferedReader inputReader;
    final ExecutorService executor; // thread pool to handle the incoming queries without blocking

    public DailyExpenditureProcessor(String histtollsfile) {
        Path path = Paths.get(histtollsfile);
        try {
            inputReader = Files.newBufferedReader(path, StandardCharsets.US_ASCII);
        } catch (IOException e) {
            throw new RuntimeException("Reading file " + histtollsfile + " failed: ", e);
        }

        ds = new ComboPooledDataSource();
        try {
            ds.setDriverClass( "org.postgresql.Driver" ); //loads the jdbc driver
        } catch (PropertyVetoException e) {
            throw new RuntimeException("No driver class was found? ", e);
        }
        ds.setJdbcUrl(url);
        ds.setUser(user);
        ds.setPassword(password);

        ds.setMinPoolSize(5);
        ds.setAcquireIncrement(5);
        ds.setMaxPoolSize(20);

        executor = Executors.newFixedThreadPool(20);

//        preloadData();
    }

    public static void main(String[] args) throws InterruptedException {
        DailyExpenditureProcessor dep = new DailyExpenditureProcessor("/home/van/dipl/linearRoad/input-downloaded/histtolls.txt");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            dep.handleQuery(new DailyExpenditureQuery((byte) 0, (short) 10, (i % 50) + 1, (byte) 0, 999, (byte) ((i % 67) + 1)));
        }
        System.out.println("Sleeping " + 5000 + " milliseconds");
//        Thread.sleep(5000);
        dep.executor.shutdown();
        dep.executor.awaitTermination(100, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - start;
        System.out.println("duration = " + duration);
    }

    public void handleQuery(final DailyExpenditureQuery deq) {
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
//                    System.out.println(deq + " had tolls: " + rs.getInt("tolls"));
                } catch (SQLException e) {
                    System.out.println("Query " + deq + " failed with: " + e);
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            System.out.println("Closing connection failed: " + e); // just print, ignore
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
