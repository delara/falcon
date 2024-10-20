package com.edgestream.worker.testing;

import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;


public class MessagingService {


    ActiveMQServerControl serverControl;

    public MessagingService() throws Exception {

        ObjectName on = ObjectNameBuilder.DEFAULT.getActiveMQServerObjectName();
        HashMap env =  new HashMap();

        String[] creds = {"guest","guest"};

        env.put(JMXConnector.CREDENTIALS,creds);
        JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi"), env);
        MBeanServerConnection mbsc = connector.getMBeanServerConnection();
        ActiveMQServerControl serverControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, ActiveMQServerControl.class, false);
        for (String queueName : serverControl.getQueueNames()) {
            System.out.println(queueName);
        }
        connector.close();
    }

    public static void main(String[] args) throws Exception {

        MessagingService msgServer = new MessagingService();

    }


}
