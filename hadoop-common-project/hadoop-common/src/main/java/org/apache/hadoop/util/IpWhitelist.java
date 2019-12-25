package org.apache.hadoop.util;

import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public interface IpWhitelist {
  boolean isIn(String ipAddress);

  public static class AllIpWhitelist implements IpWhitelist {
    @Override
    public boolean isIn(String ipAddress) {
      return true;
    }
  }

  public static class NullIpWhitelist implements IpWhitelist {
    @Override
    public boolean isIn(String ipAddress) {
      return false;
    }
  }

  public static class FileConfiguredIpWhitelist implements IpWhitelist {

    private static final Logger LOG =
        LoggerFactory.getLogger(FileConfiguredIpWhitelist.class.getName());

    private static ConcurrentHashMap<String, FileConfiguredIpWhitelist> instances =
        new ConcurrentHashMap<String, FileConfiguredIpWhitelist>();

    public static synchronized FileConfiguredIpWhitelist getInstance(String configFileName)
        throws IOException {
      FileConfiguredIpWhitelist whiteList = instances.get(configFileName);
      if (whiteList == null) {
        whiteList = new FileConfiguredIpWhitelist(configFileName);
        instances.put(configFileName, whiteList);
      }
      return whiteList;
    }

    private String configFileName;
    private ScheduledExecutorService scheduledExecutor;
    private AtomicReference<HashSet<String>> ipWhiteList = new AtomicReference<HashSet<String>>();

    private FileConfiguredIpWhitelist(String configFileName) throws IOException {
      this.configFileName = configFileName;
      ipWhiteList.set(reloadIpWhiteList());

      scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
      scheduledExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          try {
            LOG.info("reload ip whitelist - start");
            ipWhiteList.set(reloadIpWhiteList());
            LOG.info("reload ip whitelist - end");
          } catch (Exception exception) {
            LOG.error("reload ip whitelist - failure", exception);
          }
        }
      }, 5, 5, TimeUnit.MINUTES);
    }

    private HashSet<String> reloadIpWhiteList() throws IOException {
      String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
      if (hadoopConfDir == null || hadoopConfDir.isEmpty()) {
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null || hadoopHome.isEmpty()) {
          hadoopConfDir = "/etc/hadoop";
        } else {
          hadoopConfDir = hadoopHome + "/etc/hadoop";
        }
      }
      HashSet<String> ipWhiteList = new HashSet<String>();
      BufferedReader fileReader =
          new BufferedReader(new FileReader(new File(hadoopConfDir, configFileName)));
      String line = fileReader.readLine();
      while (line != null) {
        String trimLine = line.trim();
        if (!trimLine.startsWith("#") && !trimLine.isEmpty()) {
          if (!InetAddresses.isInetAddress(trimLine)) {
            throw new IllegalArgumentException(trimLine + " is not a valid ip address");
          } else {
            ipWhiteList.add(trimLine);
          }
        }

        line = fileReader.readLine();
      }
      return ipWhiteList;
    }

    @Override
    public boolean isIn(String ipAddress) {
      return ipWhiteList.get().contains(ipAddress);
    }
  }
}


