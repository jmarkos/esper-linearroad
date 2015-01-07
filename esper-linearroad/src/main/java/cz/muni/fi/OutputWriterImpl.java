package cz.muni.fi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import cz.muni.fi.eventtypes.AccidentNotificationEvent;
import cz.muni.fi.eventtypes.AccountBalanceResponse;
import cz.muni.fi.eventtypes.DailyExpenditureResponse;
import cz.muni.fi.eventtypes.TollNotificationEvent;
import org.apache.log4j.Logger;

/**
 * Writes output events to their respective files, also fills in the emit time (= outputTime)
 * based on the current DataDriver second.
 */
public class OutputWriterImpl implements OutputWriter {

    private static org.apache.log4j.Logger log = Logger.getLogger(OutputWriterImpl.class);

    public String outputDirectory;
    public String tollNotificationsFileName = "tollnotifications.txt";
    public String accidentNotificationsFileName = "accidentnotification.txt";
    public String accountBalanceResponsesFileName = "accountbalances.txt";
    public String dailyExpenditureResponsesFileName = "dailyexpenditures.txt";

    BufferedWriter tollWriter;
    BufferedWriter accidentWriter;
    BufferedWriter accountBalancesWriter;
    BufferedWriter dailyExpendituresWriter;

    DataDriver dataDriver;

    public OutputWriterImpl(DataDriver dataDriver, String _outputDirectory) {
        this.outputDirectory = _outputDirectory;
        try {
            tollWriter = new BufferedWriter(new FileWriter(new File(outputDirectory + tollNotificationsFileName)));
            accidentWriter = new BufferedWriter(new FileWriter(new File(outputDirectory + accidentNotificationsFileName)));
            accountBalancesWriter = new BufferedWriter(new FileWriter(new File(outputDirectory + accountBalanceResponsesFileName)));
            dailyExpendituresWriter = new BufferedWriter(new FileWriter(new File(outputDirectory + dailyExpenditureResponsesFileName)));
        } catch (IOException e) {
            throw new RuntimeException("Creating files for output events failed. ", e);
        }
        this.dataDriver = dataDriver;
    }

    public void close() {
        try {
            tollWriter.close();
            accidentWriter.close();
            accountBalancesWriter.close();
            dailyExpendituresWriter.close();
        } catch (IOException e) {
            throw new RuntimeException("Closing files with output events failed. ", e);
        }
    }

    public void outputAccidentNotification(AccidentNotificationEvent ane) {
        ane.setOutputTime((short) dataDriver.lastSecond);
        log.debug("Writing accident notification " + ane);
        try {
            accidentWriter.write(ane.toFileString());
            accidentWriter.newLine();
        } catch (IOException e) {
            log.error("Writing accident notification failed: ", e);
        }
    }

    public void outputTollNotification(TollNotificationEvent tne) {
        tne.setOutputTime((short) dataDriver.lastSecond);
        log.debug("Writing toll notification " + tne);
        try {
            tollWriter.write(tne.toFileString());
            tollWriter.newLine();
        } catch (IOException e) {
            log.error("Writing toll notification failed: ", e);
        }
    }

    public synchronized void outputDailyExpenditureResponse(DailyExpenditureResponse der) {
        der.setOutputTime((short) dataDriver.lastSecond);
        log.debug("Writing daily expenditure response " + der);
        try {
            dailyExpendituresWriter.write(der.toFileString());
            dailyExpendituresWriter.newLine();
        } catch (IOException e) {
            log.error("Writing daily expenditure response failed: ", e);
        }
    }

    public void outputAccountBalanceResponse(AccountBalanceResponse abr) {
        abr.setOutputTime((short) dataDriver.lastSecond);
        log.debug("Writing account balance response " + abr);
        try {
            accountBalancesWriter.write(abr.toFileString());
            accountBalancesWriter.newLine();
        } catch (IOException e) {
            log.error("Writing account balance response failed: ", e);
        }
    }

}
