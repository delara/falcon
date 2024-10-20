package com.edgestream.worker.testing;

import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import static java.lang.Boolean.FALSE;

public class TestQueueBroker {




    public static void main (String[] args) throws Exception {



        String broker_ip = "192.168.0.240";
        String topology_address_tp001 = "primary_topology_pipeline_TP_001";
        String topology_address_tp002 = "primary_topology_pipeline_TP_002";

        String new_queue_name1 = "_to_be_forwarded1";
        String new_queue_name2 = "_to_be_forwarded2";
        String queueFilter = "";
        Boolean is_durable = FALSE;
        String queueType = "ANYCAST";



        /***********Connection  1******/
        ActiveMQServerControl serverControl1;
        MBeanServerConnection connection1 = null;


        //Setup the mBean server that we will use for all operations
        JMXConnector connector1 = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + broker_ip + ":3000/jmxrmi"));
        connector1.connect();
        connection1 = connector1.getMBeanServerConnection();

        String beanName1 = "org.apache.activemq.artemis:broker=" + "\"merlin01\"";
        ObjectName server_mbeanName1 = new ObjectName(beanName1);

        serverControl1 = MBeanServerInvocationHandler.newProxyInstance(connection1, server_mbeanName1, ActiveMQServerControl.class, true);
        serverControl1.createAddress(topology_address_tp001, "ANYCAST");
        serverControl1.createQueue(topology_address_tp001, topology_address_tp001 + "::" + new_queue_name1, queueFilter, is_durable, queueType);



        /***********Connection  2********/
        ActiveMQServerControl serverControl2;
        MBeanServerConnection connection2 = null;


        //Setup the mBean server that we will use for all operations
        JMXConnector connector2 = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + broker_ip + ":3000/jmxrmi"));
        connector2.connect();
        connection2 = connector2.getMBeanServerConnection();

        String beanName2 = "org.apache.activemq.artemis:broker=" + "\"merlin01\"";
        ObjectName server_mbeanName2 = new ObjectName(beanName2);

        serverControl2 = MBeanServerInvocationHandler.newProxyInstance(connection2, server_mbeanName2, ActiveMQServerControl.class, true);
        serverControl2.createAddress(topology_address_tp002, "ANYCAST");
        serverControl2.createQueue(topology_address_tp002, topology_address_tp002 + "::" + new_queue_name2, queueFilter, is_durable, queueType);






    }


}
