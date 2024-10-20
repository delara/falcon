package com.edgestream.worker.storage;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;

public class EventTimeWindowStateObject implements Serializable {
    String key;
    ArrayList<String> registeredSources;
    long timeState;
    ArrayList<Long> windowStartTimes;
    HashMap<String, ArrayList<String>> sourceWatermarks;
    HashMap<String, ArrayList<String>> sourceWindows;

    private void triggerLocalClock(long windowFreq) {
        long clockUpdateFreq = 1; // in millis
        new Thread(() -> {
            do {
                long timeBeforeSleep = System.currentTimeMillis();
                try {
                    Thread.sleep(clockUpdateFreq);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                this.timeState += clockUpdateFreq;
                long timeAfterSleep = System.currentTimeMillis();
                this.timeState += (timeAfterSleep-timeBeforeSleep);
//                System.out.println("[Operator Clock Thread For Key " + key + "] Updated time state to: " + timeState);
                if (this.timeState >= windowStartTimes.get(windowStartTimes.size()-1) + windowFreq) {
                    System.out.println("Adding a new windowStartTime");
                    windowStartTimes.add(this.timeState);
                    System.out.println("Window start times: " + windowStartTimes);
                }
            } while (true);
        }).start();
    }

    public EventTimeWindowStateObject(String key) {
        this.key = key;
        registeredSources = new ArrayList<>();
        sourceWatermarks = new HashMap<>();
        sourceWindows = new HashMap<>();
        windowStartTimes = new ArrayList<>();
    }




}
