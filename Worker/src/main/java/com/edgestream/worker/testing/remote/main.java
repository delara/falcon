package com.edgestream.worker.testing.remote;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class main {


    public static void main (String[] args){



        File file = new File("C:\\classes\\");
        String className = "com.starstream.testing.remote.BlueBall";
        try
        {
            URL fileURL = file.toURI().toURL();
            URL[] urls = new URL[]{fileURL};

            //load this folder into Class loader
            ClassLoader cl = new URLClassLoader(urls);

            //load the Address class in 'c:\\other_classes\\'
            Class cls = cl.loadClass(className);



            URLClassLoader loader = new URLClassLoader(urls);
            Object obj = Class.forName(className, true, loader).newInstance();
            System.out.println("Object is: \"" + obj + "\"");

            Ball blueBall = (Ball) Class.forName(className, true, loader).newInstance();



            System.out.println(blueBall.getColor());


        }
        catch (MalformedURLException ex)
        {
            ex.printStackTrace();
        }
        catch (InstantiationException ex)
        {
            ex.printStackTrace();
        }
        catch (IllegalAccessException ex)
        {
            ex.printStackTrace();
        }
        catch (ClassNotFoundException ex)
        {
            ex.printStackTrace();
        }


    }
}
