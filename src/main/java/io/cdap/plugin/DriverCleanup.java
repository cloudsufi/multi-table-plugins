/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin;

import io.cdap.cdap.etl.api.Destroyable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Hashtable;
import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * class to de-register driver
 */
public class DriverCleanup implements Destroyable {
  private static final Logger LOG = LoggerFactory.getLogger(DriverCleanup.class);
  private final JDBCDriverShim driverShim;
  private final Class<? extends Driver> driverClass;

  DriverCleanup(Class<? extends Driver> driverClass, @Nullable JDBCDriverShim driverShim) {
    this.driverClass = driverClass;
    this.driverShim = driverShim;
  }

  public void destroy() {
    if (driverShim != null) {
      try {
        DriverManager.deregisterDriver(driverShim);
      } catch (SQLException e) {
        LOG.warn("Unable to de-register jdbc driver. This may lead to memory leaks in the CDAP sandbox.");
      }
    }

    ClassLoader pluginClassLoader = driverClass.getClassLoader();
    if (pluginClassLoader == null) {
      // This could only be null if the classLoader is the Bootstrap/Primordial classloader. This should never be the
      // case since the driver class is always loaded from the plugin classloader.
      LOG.warn("PluginClassLoader is null. Cleanup not necessary.");
      return;
    }
    shutDownMySQLAbandonedConnectionCleanupThread(pluginClassLoader);
    unregisterOracleMBean(pluginClassLoader);
  }

  /**
   * Shuts down a cleanup thread com.mysql.jdbc.AbandonedConnectionCleanupThread that mysql driver fails to destroy
   * If this is not done, the thread keeps a reference to the classloader, thereby causing OOMs or too many open files
   *
   * @param classLoader the unfiltered classloader of the jdbc driver class
   */
  private static void shutDownMySQLAbandonedConnectionCleanupThread(ClassLoader classLoader) {
    try {
      Class<?> mysqlCleanupThreadClass;
      try {
        mysqlCleanupThreadClass = classLoader.loadClass("com.mysql.jdbc.AbandonedConnectionCleanupThread");
      } catch (ClassNotFoundException e) {
        // Ok to ignore, since we may not be running mysql
        LOG.trace("Failed to load MySQL abandoned connection cleanup thread class. Presuming DB App is " +
                    "not being run with MySQL and ignoring", e);
        return;
      }
      Method shutdownMethod = mysqlCleanupThreadClass.getMethod("shutdown");
      shutdownMethod.invoke(null);
      LOG.info("Successfully shutdown MySQL connection cleanup thread.");
    } catch (Throwable e) {
      // cleanup failed, ignoring silently with a log, since not much can be done.
      LOG.warn("Failed to shutdown MySQL connection cleanup thread. Ignoring.", e);
    }
  }

  private static void unregisterOracleMBean(ClassLoader classLoader) {
    try {
      classLoader.loadClass("oracle.jdbc.driver.OracleDriver");
    } catch (ClassNotFoundException e) {
      LOG.info("Oracle JDBC Driver not found. Presuming that the DB App is not being run with an Oracle DB. " +
                  "Not attempting to cleanup Oracle MBean.");
      return;
    }
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Hashtable<String, String> keys = new Hashtable<>();
    keys.put("type", "diagnosability");
    keys.put("name",
             classLoader.getClass().getName() + "@" + Integer.toHexString(classLoader.hashCode()).toLowerCase());
    ObjectName oracleJdbcMBeanName;
    try {
      oracleJdbcMBeanName = new ObjectName("com.oracle.jdbc", keys);
    } catch (MalformedObjectNameException e) {
      // This should never happen, since we're constructing the ObjectName correctly
      LOG.info("Exception while constructing Oracle JDBC MBean Name. Aborting cleanup.", e);
      return;
    }
    try {
      mbs.getMBeanInfo(oracleJdbcMBeanName);
    } catch (InstanceNotFoundException e) {
      LOG.info("Oracle JDBC MBean not found. No cleanup necessary.");
      return;
    } catch (IntrospectionException | ReflectionException e) {
      LOG.info("Exception while attempting to retrieve Oracle JDBC MBean. Aborting cleanup.", e);
      return;
    }

    try {
      mbs.unregisterMBean(oracleJdbcMBeanName);
      LOG.info("Oracle MBean unregistered successfully.");
    } catch (InstanceNotFoundException | MBeanRegistrationException e) {
      LOG.info("Exception while attempting to cleanup Oracle JDBCMBean. Aborting cleanup.", e);
    }
  }
}
