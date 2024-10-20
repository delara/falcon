package com.edgestream.worker.testing;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

public class BrokerTest {
    ActiveMQServerControl serverControl;
    MBeanServerConnection connection;

    String broker_ip =  "localhost";


    public static void main(String[] args) {

try {

    BrokerTest brokerTest = new BrokerTest();

    brokerTest.setActiveMQServerControl();
    String attributeAddress1 = "cloud";
    String new_attribute_queue_name1 = "cloud_queue";
    //brokerTest.createAddressAndQueue(attributeAddress1, new_attribute_queue_name1, brokerTest);

    String attributeAddress2 = "edge";
    String new_attribute_queue_name2 = "edge_queue";
    //brokerTest.createAddressAndQueue(attributeAddress2, new_attribute_queue_name2, brokerTest);


} catch (Exception e) {
    e.printStackTrace();
}

    }

    public void createAddressAndQueue(String attributeAddress1, String new_attribute_queue_name, BrokerTest brokerTest) {
        try {
            brokerTest.serverControl.deleteAddress(attributeAddress1);

        String address = brokerTest.serverControl.createAddress(attributeAddress1, "ANYCAST");

        System.out.println(address);
        System.out.println(attributeAddress1);


        AddressControl addressControl = brokerTest.getAddressController(attributeAddress1);
        addressControl.pause();

        System.out.println(addressControl.getRoutingTypes());

        System.out.println(addressControl.getAddress());
        addressControl.pause();

        if (addressControl.isPaused()) {

            System.out.println("Address is paused");

        }


        addressControl.resume();

        if (!addressControl.isPaused()) {
            System.out.println("Address is not paused");

        }

        Boolean is_durable = false;
        String queueType = "ANYCAST";


        brokerTest.serverControl.createQueue(attributeAddress1, queueType, new_attribute_queue_name, null, is_durable, 10, false, false);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void  setActiveMQServerControl(){
        //Setup the mBean server that we will use for all operations
        JMXConnector connector = null;
        try {
            connector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + this.broker_ip + ":3000/jmxrmi"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            connector.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
        MBeanServerConnection connection = null;
        try {
            connection = connector.getMBeanServerConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String beanName = "org.apache.activemq.artemis:broker=" + "\"pritish1\"";
        ObjectName server_mbeanName = null;
        try {
            server_mbeanName = new ObjectName(beanName);
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        }
        this.serverControl = MBeanServerInvocationHandler.newProxyInstance(connection, server_mbeanName, ActiveMQServerControl.class, true);


    }


    public AddressControl getAddressController(String address){

        AddressControl addressControl = null;
        try {
            ObjectName on = ObjectNameBuilder.create("org.apache.activemq.artemis", "pritish1").getAddressObjectName(SimpleString.toSimpleString(address));
            JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + this.broker_ip + ":3000/jmxrmi"));
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();


            addressControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, AddressControl.class, false);

            //System.out.println("Created a controller for address: " + addressControl.getAddress());
        }catch(Exception e){
            e.printStackTrace();

        }

        return addressControl;

    }
}
