package cz.muni.fi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import cz.muni.fi.eventtypes.LRBEvent;
import org.apache.commons.io.filefilter.WildcardFileFilter;

/**
 * Assumes the inputDir contains files cardatapoints.out0 ... cardatapoints.outN
 * Creates a single output file, sorted by time, with xway correcly set
 */
public class InputMerger {

    public static String inputDir = "/home/van/dipl/lroad_data/5/";
    public static String outputFileName = "merged.out";

    public static void main(String[] args) throws IOException {
        if (args.length != 0) {
            inputDir = args[0];
        }

        // find the files to merge
        File inputDirFile = new File(inputDir);
        FileFilter cardatapointsFilter = new WildcardFileFilter("cardatapoints.out*");
        File[] filesToMerge = inputDirFile.listFiles(cardatapointsFilter);

        Arrays.sort(filesToMerge);

        if (filesToMerge.length == 1) {
            System.out.println("Nothing to merge, bye.");
            return;
        }

        File outputFile = new File(inputDir + outputFileName);
        BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFile));

        BufferedReader[] readers = new BufferedReader[filesToMerge.length];
        for (int i = 0; i < filesToMerge.length; i++) {
            readers[i] = new BufferedReader(new FileReader(filesToMerge[i]));
        }

        String line;
        // leftOver events are the events which were read during second i, but have time i+1
        // when they are encountered, we stop reading further, remember them and write them during i+1 second
        ArrayList<LRBEvent> leftOverEvents = new ArrayList<LRBEvent>(filesToMerge.length);
        for (int i = 0; i < 10800; i++) {
            for (LRBEvent leftOverEvent : leftOverEvents) {
                outputWriter.write(leftOverEvent.toFileString());
                outputWriter.newLine();
            }
            leftOverEvents.clear();
            for (int j = 0; j < readers.length; j++) {
                BufferedReader reader = readers[j];
                while ((line = reader.readLine()) != null) {
                    String[] data = line.split(",");
                    LRBEvent e = new LRBEvent(data);
                    if (e.type == 0) {
                        e.xway = (byte) j;
                    }
                    if (e.time == i) {
                        // write it to outputFile
                        outputWriter.write(e.toFileString());
                        outputWriter.newLine();
                    } else { // future event, stop
                        leftOverEvents.add(e);
                        break;
                    }
                }
            }
            if (i % 100 == 0) {
                System.out.println("Processing second " + i);
            }
        }

        for (int i = 0; i < filesToMerge.length; i++) {
            readers[i].close();
        }
        outputWriter.close();
    }

}
