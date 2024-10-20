package com.edgestream.worker.testing;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class TestAddressControl {




    public static void main (String[] args) throws Exception {



        String broker_ip = "192.168.0.240";

        try {
            //String beanName1 = "org.apache.activemq.artemis:broker=" + "\"merlin01\"";
            //ObjectName server_mbeanName1 = new ObjectName(beanName1);

            //ObjectName on = new ObjectName(beanName1);

            //ObjectName on = ObjectNameBuilder.DEFAULT.getAddressObjectName(SimpleString.toSimpleString("primary_topology_pipeline_TP_001"));


            ObjectName on =  ObjectNameBuilder.create("org.apache.activemq.artemis","merlin01").getAddressObjectName(SimpleString.toSimpleString("primary_topology_pipeline_TP_001"));
            //ObjectName on = ObjectNameBuilder.DEFAULT.getActiveMQServerObjectName();
            JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + broker_ip + ":3000/jmxrmi"));
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();


            AddressControl addressControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, AddressControl.class, false);



            for (String queueName : addressControl.getQueueNames()) {
                System.out.println(queueName);
            }


            connector.close();

        }catch(Exception e){

            e.printStackTrace();

        }

/*


        AddressControl addressControl1;
        MBeanServerConnection connection1 = null;


        //Setup the mBean server that we will use for all operations
        JMXConnector connector1 = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + broker_ip + ":3000/jmxrmi"));
        connector1.connect();
        connection1 = connector1.getMBeanServerConnection();

        String beanName1 = "org.apache.activemq.artemis:broker=" + "\"merlin01\"";
        ObjectName server_mbeanName1 = new ObjectName(beanName1);

        //addressControl1 = MBeanServerInvocationHandler.newProxyInstance(connection1, server_mbeanName1, AddressControl.class, true);



        System.out.println(connection1.getDomains());

        for(String domains : connection1.getDomains()){

            System.out.println(domains);
        }





 */
        /*
        serverControl1 = MBeanServerInvocationHandler.newProxyInstance(connection1, server_mbeanName1, AddressControl.class, true);
        serverControl1.createAddress(topology_address_tp001, "ANYCAST");
        serverControl1.createQueue(topology_address_tp001, topology_address_tp001 + "::" + new_queue_name1, queueFilter, is_durable, queueType);

        */






    }


}
