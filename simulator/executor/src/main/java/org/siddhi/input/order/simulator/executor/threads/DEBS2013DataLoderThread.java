/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.siddhi.input.order.simulator.executor.threads;

import com.google.common.base.Splitter;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This data loader thread loads DEBS 2013 Grand Challenge data set which is accessible from
 * http://www.orgs.ttu.edu/debs2013/index.php?goto=cfchallengedetails
 */
public class DEBS2013DataLoderThread implements DataLoaderThread{
	private String fileName;
	private static Splitter splitter = Splitter.on(',');
	private LinkedBlockingQueue<Object> eventBufferList;
	private BufferedReader br;
	private int count;
    private long eventLimit;//This many events will be read from the stream data set
    private int sensorIDField = -1;
    private boolean calibrated;
    private String FILE_PATH = "sensors.xml";
    private HashMap<Integer, Integer> sensorMap;

    /**
     * The data loader just uses a particular file to load data. Since an event count is not specified in this constructor
     * the full amount of events will be read from the file line by line.
     * @param fileName - input data file
     */
	public DEBS2013DataLoderThread(String fileName){
        this(fileName, -1l);
	}

    /**
     * The data loader reads from a file upto a specified number of events. It outputs only single event stream which
     * corresponds to the entire data set.
     * @param fileName - the input data file
     * @param eventCount - number of events to read
     */
    public DEBS2013DataLoderThread(String fileName, long eventCount){
        this.fileName = fileName;
        eventBufferList = new LinkedBlockingQueue<Object>();
        this.eventLimit = eventCount;
    }

    /**
     * The data loader reads from a file upto a specified number of events. It outputs only single event stream which
     * corresponds to the entire data set.
     * @param fileName - the input data file
     * @param eventCount - number of events to read
     * @param sensorIDField - the index of the field which carries the sernsor id
     */
    public DEBS2013DataLoderThread(String fileName, long eventCount, int sensorIDField){
        this.fileName = fileName;
        eventBufferList = new LinkedBlockingQueue<Object>();
        this.eventLimit = eventCount;
        this.sensorIDField = sensorIDField;
    }

    /**
     * Note that the first field listed in the events output from the data loader must correspond to the timestamp used
     * for ordering the events. This is an assumption which must be followed in all use cases. The timestamp must be a
     * long value.
     */
	public void run(){
        if(sensorIDField == -1) {
            //This is just replaying the entire data set with disorder
            runSingleStream();
        }else {
            runSensors();
        }
	}

    /**
     * We need to run this only if we do not have a sensors.xml file on the current directory.
     */
    public void runSensors() {
        if (!isCalibrated()) {
            System.out.println("Extracting the sensor data from the input file.");
            calibrate();
        }
        System.out.println("Start the simulation with the file: " + FILE_PATH);
        loadSensorInformation();

        //Once we load the sensor information we can start pumping data.
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();

            while (line != null) {
                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                Integer sid = Integer.parseInt(dataStrIterator.next()); //sensor id
                String ts = dataStrIterator.next(); //Timestamp in pico seconds
                String x = dataStrIterator.next();
                String y = dataStrIterator.next();
                String z = dataStrIterator.next();
                String v_abs = dataStrIterator.next();
                String a_abs = dataStrIterator.next();
                String vx = dataStrIterator.next();
                String vy = dataStrIterator.next();
                String vz = dataStrIterator.next();
                String ax = dataStrIterator.next();
                String ay = dataStrIterator.next();
                String az = dataStrIterator.next();

                Object[] eventData = null;
                Integer it = sensorMap.get(sid);
                try{
                    eventData = new Object[]{
                            sid,
                            Long.parseLong(ts) + (sensorMap.get(sid) * 1000000000l), //Since this value is in pico seconds we
                                                                                 // have to multiply by 1000M (10^9) to make milliseconds to picoseconds
                            Integer.parseInt(x), //This can be represented by two bytes
                            Integer.parseInt(y),
                            Integer.parseInt(z),
                            Integer.parseInt(v_abs),
                            Integer.parseInt(a_abs),
                            Integer.parseInt(vx),
                            Integer.parseInt(vy),
                            Integer.parseInt(vz),
                            Integer.parseInt(ax),
                            Integer.parseInt(ay),
                            Integer.parseInt(az)
                            };
                }catch(NumberFormatException e){
                    //e.printStackTrace();
                    //If we find a discrepancy in converting data, then we have to discard that
                    //particular event.
                    line = br.readLine();
                    continue;
                }

                //We keep on accumulating data on to the event queue.
                //This will get blocked if the space required is not available.
                eventBufferList.put(eventData);
                line = br.readLine();
                count++;

                if(count >= eventLimit){
                    break;
                }
            }
            System.out.println("Total amount of events read : " + count);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void loadSensorInformation() {
        sensorMap = new HashMap<Integer, Integer>();

        File fXmlFile = new File(FILE_PATH);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = null;
        Document doc = null;

        try {
            dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(fXmlFile);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        doc.getDocumentElement().normalize();
        NodeList nList = doc.getElementsByTagName("sensor");

        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);

            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                int sensorID = Integer.parseInt(eElement.getAttribute("id"));
                NodeList clockDrift = eElement.getElementsByTagName("clock-drift");
                Element driftElement = (Element) clockDrift.item(0);
                int drift = Integer.parseInt(driftElement.getAttribute("drift"));
                sensorMap.put(sensorID, drift);
            }
        }

        System.out.println("There are " + sensorMap.keySet().size() + " sensors.");
    }

    /**
     * The following method reads through the input data set and identifies the number of sensors present within it. Then
     * it creates a file called sensors.xml with all the sensor information and clock dirft valuse set to zero.
     */
    private void calibrate() {
        TreeSet<Integer> sensorIDs = new TreeSet<Integer>();
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            int count = 0;

            while (line != null) {
                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                count = 0;

                while(count < sensorIDField) {
                    count++;
                    dataStrIterator.next();
                }

                String sensorID = dataStrIterator.next();
                sensorIDs.add(Integer.parseInt(sensorID));

                line = br.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ec){
            ec.printStackTrace();
        }

        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            Element rootElement = doc.createElement("sensors");
            doc.appendChild(rootElement);
            Comment comment = doc.createComment("The drift value is in miliseconds.");
            rootElement.getParentNode().insertBefore(comment, rootElement);

            Iterator<Integer> itrSensor = sensorIDs.iterator();
            while(itrSensor.hasNext()){
                int sensorID = itrSensor.next();
                Element sensorElement = doc.createElement("sensor");
                Attr attr = doc.createAttribute("id");
                attr.setValue(""+sensorID);
                sensorElement.setAttributeNode(attr);
                rootElement.appendChild(sensorElement);

                Element sensorDriftElement = doc.createElement("clock-drift");
                Attr attr2 = doc.createAttribute("drift");
                attr2.setValue("0");
                sensorDriftElement.setAttributeNode(attr2);
                sensorElement.appendChild(sensorDriftElement);
            }

            // write the content into xml file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = null;
            try {
                transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            } catch (TransformerConfigurationException e) {
                e.printStackTrace();
            }
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(FILE_PATH));

            // Output to console for testing
            // StreamResult result = new StreamResult(System.out);

            try {
                transformer.transform(source, result);
            } catch (TransformerException e) {
                e.printStackTrace();
            }
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public void runSingleStream(){
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();

            while (line != null) {
                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String sid = dataStrIterator.next(); //sensor id
                String ts = dataStrIterator.next(); //Timestamp in pico seconds
                String x = dataStrIterator.next();
                String y = dataStrIterator.next();
                String z = dataStrIterator.next();
                String v_abs = dataStrIterator.next();
                String a_abs = dataStrIterator.next();
                String vx = dataStrIterator.next();
                String vy = dataStrIterator.next();
                String vz = dataStrIterator.next();
                String ax = dataStrIterator.next();
                String ay = dataStrIterator.next();
                String az = dataStrIterator.next();

                Object[] eventData = null;

                try{
                    eventData = new Object[]{
                            Integer.parseInt(sid),
                            Long.parseLong(ts),
                            Integer.parseInt(x), //This can be represented by two bytes
                            Integer.parseInt(y),
                            Integer.parseInt(z),
                            Integer.parseInt(v_abs),
                            Integer.parseInt(a_abs),
                            Integer.parseInt(vx),
                            Integer.parseInt(vy),
                            Integer.parseInt(vz),
                            Integer.parseInt(ax),
                            Integer.parseInt(ay),
                            Integer.parseInt(az)
                    };
                }catch(NumberFormatException e){
                    //e.printStackTrace();
                    //If we find a discrepancy in converting data, then we have to discard that
                    //particular event.
                    line = br.readLine();
                    continue;
                }

                //We keep on accumulating data on to the event queue.
                //This will get blocked if the space required is not available.
                eventBufferList.put(eventData);
                line = br.readLine();
                count++;

                if(count >= eventLimit){
                    break;
                }
            }
            System.out.println("Total amount of events read : " + count);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("Now exiting from data loader thread.");
    }

    public LinkedBlockingQueue<Object> getEventBuffer(){
        return eventBufferList;
    }

    protected long convert(String data) {
        String dateTime = (String) data;
        try {
            int y = Integer.parseInt(dateTime.substring(0, 4));
            int m = Integer.parseInt(dateTime.substring(5, 7));
            --m;
            int d = Integer.parseInt(dateTime.substring(8, 10));
            int h = Integer.parseInt(dateTime.substring(11, 13));
            int mm = Integer.parseInt(dateTime.substring(14, 16));
            int s = Integer.parseInt(dateTime.substring(17));

            CachedCalendar.set(y, m, d, h, mm, s);

            if (CachedCalendar.get(Calendar.YEAR) != y) {
                return 0;
            }
            if (CachedCalendar.get(Calendar.MONTH) != m) {
                return 0;
            }
            if (CachedCalendar.get(Calendar.DATE) != d) {
                return 0;
            }

            if (h < 0 || m > 23) {
                return 0;
            }
            if (mm < 0 || mm > 59) {
                return 0;
            }
            if (s < 0 || s > 59) {
                return 0;
            }

            return CachedCalendar.getTime().getTime();
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    private static final Calendar CachedCalendar = new GregorianCalendar();
    static {
        CachedCalendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        CachedCalendar.clear();
    }

    public boolean isCalibrated() {
        File file = new File(FILE_PATH);

        if(file.exists()) {
            return true;
        }else{
            return false;
        }
    }
}
