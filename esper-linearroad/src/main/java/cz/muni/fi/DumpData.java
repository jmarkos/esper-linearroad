package cz.muni.fi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

// helper class, LRB input generator creates the final data in the table named 'input', and simply dumps it into a file,
// which means it's not ordered by time, this class retrieves the data by time (there is index on time, so it's not slow)
public class DumpData {

    static String url = "jdbc:postgresql://localhost/postgres";
    static String user = "lrb";
    static String password = "lrb";

    public static void main(String[] args) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/data/cardatapoints_L5.out")));
        Connection connection = DriverManager.getConnection(url, user, password);
        Statement st = connection.createStatement();
        ResultSet rs = null;
        for (int i = 0; i < 10800; i++) {
            rs = st.executeQuery("SELECT * from input where time=" + i);
            while(rs.next()) {
                StringBuilder line = new StringBuilder();
                line.append(rs.getInt(1)).append(",");
                line.append(rs.getInt(2)).append(",");
                line.append(rs.getInt(3)).append(",");
                line.append(rs.getInt(4)).append(",");
                line.append(rs.getInt(5)).append(",");
                line.append(rs.getInt(6)).append(",");
                line.append(rs.getInt(7)).append(",");
                line.append(rs.getInt(8)).append(",");
                line.append(rs.getInt(9)).append(",");
                line.append(rs.getInt(10)).append(",");
                line.append(rs.getInt(11)).append(",");
                line.append(rs.getInt(12)).append(",");
                line.append(rs.getInt(13)).append(",");
                line.append(rs.getInt(14)).append(",");
                line.append(rs.getInt(15));
                writer.write(line.toString());
                writer.newLine();
            }
            if (i % 100 == 0) {
                System.out.println("Processing second " + i);
            }
        }
        st.close();
        connection.close();
        writer.close();
    }

}
