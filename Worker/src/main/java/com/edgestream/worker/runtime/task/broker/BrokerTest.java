package com.edgestream.worker.runtime.task.broker;

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

    public ActiveMQServerControl serverControl;
    MBeanServerConnection connection;

    String broker_ip =  "10.70.20.48";


    public static void main(String[] args) throws Exception {



        BrokerTest taskAddressControllerTest = new BrokerTest();

        taskAddressControllerTest.setActiveMQServerControl();
        String attributeAddress = "topology_operator_pipeline_tpstatsv3001_for_type_P_for_attribute_any";

        taskAddressControllerTest.serverControl.deleteAddress(attributeAddress);
        String address = taskAddressControllerTest.serverControl.createAddress(attributeAddress, "ANYCAST");

        System.out.println(address);
        System.out.println(attributeAddress);



        AddressControl addressControl = taskAddressControllerTest.getAddressController(address);


        System.out.println(addressControl.getRoutingTypes());

        System.out.println(addressControl.getAddress());
        addressControl.pause();

        if(addressControl.isPaused()){

            System.out.println("Address is paused");

        }


       addressControl.resume();

       if(!addressControl.isPaused()){
            System.out.println("Address is not paused");

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

        String beanName = "org.apache.activemq.artemis:broker=" + "\"merlin01\"";
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
            ObjectName on = ObjectNameBuilder.create("org.apache.activemq.artemis", "merlin01").getAddressObjectName(SimpleString.toSimpleString(address));
            JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + this.broker_ip + ":3000/jmxrmi"));
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();


            addressControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, AddressControl.class, false);

            System.out.println("Created a controller for address: " + addressControl.getAddress());
        }catch(Exception e){
            e.printStackTrace();

        }

        return addressControl;

    }

}




