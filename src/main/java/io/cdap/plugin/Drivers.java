/*
 * Copyright © 2017 Cask Data, Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Utility methods for JDBC drivers.
 */
public final class Drivers {
  private static final Logger LOG = LoggerFactory.getLogger(Drivers.class);

  /**
   * Ensures that the JDBC Driver specified in configuration is available and can be loaded. Also registers it with
   * {@link DriverManager} if it is not already registered.
   */
  public static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> jdbcDriverClass,
                                                          String connectionString)
    throws IllegalAccessException, InstantiationException, SQLException {

    try {
      DriverManager.getDriver(connectionString);
      return new DriverCleanup(jdbcDriverClass, null);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.
      final JDBCDriverShim driverShim = new JDBCDriverShim(jdbcDriverClass.newInstance());
      try {
        Drivers.deregisterAllDrivers(jdbcDriverClass);
      } catch (NoSuchFieldException | ClassNotFoundException e1) {
        LOG.error("Unable to deregister JDBC Driver class {}", jdbcDriverClass);
      }
      DriverManager.registerDriver(driverShim);
      return new DriverCleanup(jdbcDriverClass, driverShim);
    }
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  public static void deregisterAllDrivers(Class<? extends Driver> driverClass)
    throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    Field field = DriverManager.class.getDeclaredField("registeredDrivers");
    field.setAccessible(true);
    List<?> list = (List<?>) field.get(null);
    for (Object driverInfo : list) {
      Class<?> driverInfoClass = Drivers.class.getClassLoader().loadClass("java.sql.DriverInfo");
      Field driverField = driverInfoClass.getDeclaredField("driver");
      driverField.setAccessible(true);
      Driver d = (Driver) driverField.get(driverInfo);
      if (d == null) {
        LOG.info("Found null driver object in drivers list. Ignoring.");
        continue;
      }
      LOG.info("Removing non-null driver object from drivers list.");
      ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
      if (registeredDriverClassLoader == null) {
        LOG.info("Found null classloader for default driver {}. Ignoring since this may be using system classloader.",
                  d.getClass().getName());
        continue;
      }
      // Remove all objects in this list that were created using the classloader of the caller.
      if (d.getClass().getClassLoader().equals(driverClass.getClassLoader())) {
        LOG.info("Removing default driver {} from registeredDrivers", d.getClass().getName());
        list.remove(driverInfo);
      }
    }
  }

  private Drivers() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
