package wuxian.me.hiveudf.util;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.conf.HiveConf.*;

//copy from org/apache/hadoop/hive/metastore/conf/MetastoreConf.java to fix hive version issure
public class MetastoreConf {

    private static URL hiveDefaultURL = null;
    private static URL metastoreSiteURL = null;
    private static URL hiveMetastoreSiteURL = null;
    private static URL hiveSiteURL = null;
    private static AtomicBoolean beenDumped = new AtomicBoolean();

    public static Configuration newMetastoreConf() {

        Configuration conf = new Configuration();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = MetastoreConf.class.getClassLoader();
        }
        // We don't add this to the resources because we don't want to read config values from it.
        // But we do find it because we want to remember where it is for later in case anyone calls
        // getHiveDefaultLocation().
        hiveDefaultURL = classLoader.getResource("hive-default.xml");

        // Add in hive-site.xml.  We add this first so that it gets overridden by the new metastore
        // specific files if they exist.
        hiveSiteURL = findConfigFile(classLoader, "hive-site.xml");
        if (hiveSiteURL != null) conf.addResource(hiveSiteURL);

        // Now add hivemetastore-site.xml.  Again we add this before our own config files so that the
        // newer overrides the older.
        hiveMetastoreSiteURL = findConfigFile(classLoader, "hivemetastore-site.xml");
        if (hiveMetastoreSiteURL != null) conf.addResource(hiveMetastoreSiteURL);

        // Add in our conf file
        metastoreSiteURL = findConfigFile(classLoader, "metastore-site.xml");
        if (metastoreSiteURL != null) conf.addResource(metastoreSiteURL);

        // If a system property that matches one of our conf value names is set then use the value
        // it's set to to set our own conf value.
        for (ConfVars var : ConfVars.values()) {
            if (System.getProperty(var.varname) != null) {
                //LOG.debug("Setting conf value " + var.varname + " using value " + System.getProperty(var.varname));
                conf.set(var.varname, System.getProperty(var.varname));
            }
        }

        return conf;
    }

    private static URL findConfigFile(ClassLoader classLoader, String name) {
        System.out.println("begin to findConfigFile of " + name);
        // First, look in the classpath
        URL result = classLoader.getResource(name);
        if (result == null) {
            // Nope, so look to see if our conf dir has been explicitly set
            result = seeIfConfAtThisLocation("METASTORE_CONF_DIR", name, false);
            if (result == null) {
                // Nope, so look to see if our home dir has been explicitly set
                result = seeIfConfAtThisLocation("METASTORE_HOME", name, true);
                if (result == null) {
                    // Nope, so look to see if Hive's conf dir has been explicitly set
                    result = seeIfConfAtThisLocation("HIVE_CONF_DIR", name, false);
                    if (result == null) {
                        // Nope, so look to see if Hive's home dir has been explicitly set
                        result = seeIfConfAtThisLocation("HIVE_HOME", name, true);
                        if (result == null) {
                            // Nope, so look to see if we can find a conf file by finding our jar, going up one
                            // directory, and looking for a conf directory.
                            URI jarUri = null;
                            try {
                                jarUri = MetastoreConf.class.getProtectionDomain().getCodeSource().getLocation().toURI();
                            } catch (Throwable e) {
                                //LOG.warn("Cannot get jar URI", e);
                            }
                            result = seeIfConfAtThisLocation(new File(jarUri).getParent(), name, true);
                            // At this point if we haven't found it, screw it, we don't know where it is
                            if (result == null) {
                                //LOG.info("Unable to find config file " + name);
                            }
                        }
                    }
                }
            }
        }
        System.out.println("Found configuration file " + result);
        return result;
    }

    static final String TEST_ENV_WORKAROUND = "metastore.testing.env.workaround.dont.ever.set.this.";

    private static URL seeIfConfAtThisLocation(String envVar, String name, boolean inConfDir) {
        String path = System.getenv(envVar);
        if (path == null) {
            // Workaround for testing since tests can't set the env vars.
            path = System.getProperty(TEST_ENV_WORKAROUND + envVar);
        }
        if (path != null) {
            String suffix = inConfDir ? "conf" + File.separatorChar + name : name;
            return checkConfigFile(new File(path, suffix));
        }
        return null;
    }

    private static URL checkConfigFile(File f) {
        try {
            return (f.exists() && f.isFile()) ? f.toURI().toURL() : null;
        } catch (Throwable e) {
            //LOG.warn("Error looking for config " + f, e);
            return null;
        }
    }
}
