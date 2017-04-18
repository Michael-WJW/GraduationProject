package com.michael.controller;

import org.apache.hadoop.hbase.client.Connection;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;

/**
 * Created by hadoop on 17-4-14.
 */
public class ContextClosedListener implements ServletContextListener {
    public void contextInitialized(ServletContextEvent servletContextEvent) {

    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        Connection connection = KNNQueryController.getConnection();
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
