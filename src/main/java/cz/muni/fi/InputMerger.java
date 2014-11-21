package cz.muni.fi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.filefilter.WildcardFileFilter;

// Assumes the inputDir contains files cardatapoints.out0 ... cardatapoints.outN
public class InputMerger {

    public static String inputDir = "/home/van/dipl/lroad_data/5/";
    public static String outputFileName = "merged.out";

    public static void main(String[] args) throws IOException {
        if (args.length != 0) {
            inputDir = args[0];
        }

        File inputDirFile = new File(inputDir);
        FileFilter cardatapointsFilter = new WildcardFileFilter("cardatapoints.out*");
        File[] filesToMerge = inputDirFile.listFiles(cardatapointsFilter);

        Arrays.sort(filesToMerge);

        if (filesToMerge.length == 1) {
            System.out.println("Nothing to merge, bye.");
            return;
        }

        BufferedReader[] readers = new BufferedReader[filesToMerge.length];
        for (int i = 0; i < filesToMerge.length; i++) {
            readers[i] = new BufferedReader(new FileReader(filesToMerge[i]));
        }

        // for each second, read all the events from all the files for that second
        // and write to the output file, changing the xways for the out1...outN files
        ArrayList<String[]>[] secondData = new ArrayList[filesToMerge.length];
        for (int i = 0; i < filesToMerge.length; i++) {
            secondData[i] = new ArrayList<String[]>();
        }

        String line;
        for (int i = 0; i < 10800; i++) {
            for (int j = 0; j < readers.length; j++) {
                BufferedReader reader = readers[j];
                while ((line = reader.readLine()) != null) {
                    String[] data = line.split(",");
                    // change xway
                    // TODO finish this
                }
            }
            // go over the secondData lists, write them to outputfile, clear them, repeat
        }

        /*
        1. zistit kolko ich je
        2. otvorit ich vsetky
        3. vo vsetkych okrem nulteho zamenit xway na n
        4. for i 1..10800
            ...
         */
    }

}
