package com.edgestream.worker.metrics.testing;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateTest {

    public static void main(String[] args) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
    }
}
