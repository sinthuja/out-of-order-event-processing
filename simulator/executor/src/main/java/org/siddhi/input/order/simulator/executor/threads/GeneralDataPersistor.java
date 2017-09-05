package org.siddhi.input.order.simulator.executor.threads;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class GeneralDataPersistor implements DataPersistor {
    private File persistanceFile = null;
    private BufferedWriter bw = null;
    private String NEW_LINE = "\n";

    public GeneralDataPersistor(String pathForPersistence){
        persistanceFile = new File(pathForPersistence);

        if (!persistanceFile.exists()) {
            try {
                persistanceFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        FileWriter fw = null;
        try {
            fw = new FileWriter(persistanceFile.getAbsoluteFile());
        } catch (IOException e) {
            e.printStackTrace();
        }

        bw = new BufferedWriter(fw);

        System.out.println("Done");
    }

    public void persistEvent(Object[] data) {
        int len = data.length;
        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < len; i++){
            sb.append(data[i]);
            sb.append(",");
        }

        String toWrite = sb.substring(0, sb.length()-1);

        try {
            bw.write(toWrite);
            bw.write(NEW_LINE);
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
