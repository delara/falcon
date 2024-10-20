package com.edgestream.worker.testing.ping;

import com.edgestream.worker.metrics.icmp4j.IcmpPingUtil;
import com.edgestream.worker.metrics.icmp4j.IcmpPingRequest;
import com.edgestream.worker.metrics.icmp4j.IcmpPingResponse;


// Sample class, copyright 2009 and beyond, icmp4j

public class Ping {


// the java entry point

    public static void main (final String[ ] args) throws Exception {


// request

        final IcmpPingRequest request = IcmpPingUtil.createIcmpPingRequest ();

        request.setHost ("www.google.org");


// repeat a few times

        for (int count = 1; count <= 1; count ++) {


// delegate

            final IcmpPingResponse response = IcmpPingUtil.executePingRequest (request);

            System.out.println(response.getRtt());


// log

            final String formattedResponse = IcmpPingUtil.formatResponse (response);

            System.out.println (formattedResponse);


// rest

            Thread.sleep (1000);

        }

    }

}