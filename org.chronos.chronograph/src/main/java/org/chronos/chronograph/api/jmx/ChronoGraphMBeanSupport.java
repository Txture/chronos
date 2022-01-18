package org.chronos.chronograph.api.jmx;

import org.chronos.chronograph.internal.impl.structure.graph.StandardChronoGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class ChronoGraphMBeanSupport {

    private static final Logger log = LoggerFactory.getLogger(ChronoGraphMBeanSupport.class);

    public static void registerMBeans(final StandardChronoGraph standardChronoGraph){
        try{
            // wire up the cache MBean
            ChronoGraphCacheStatistics.getInstance().setCache(standardChronoGraph.getBackingDB().getCache());
            // wire up the MBeans with the server
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            // register the transaction statistics MBean
            ObjectName objectNameTransactionStatistics = new ObjectName("org.chronos.chronograph:type=ChronoGraph.TransactionStatistics");
            mbs.registerMBean(ChronoGraphTransactionStatistics.getInstance(), objectNameTransactionStatistics);
            // register the cache statistics MBean
            ObjectName objectNameCacheStatistics = new ObjectName("org.chronos.chronograph:type=ChronoGraph.CacheStatistics");
            mbs.registerMBean(ChronoGraphCacheStatistics.getInstance(), objectNameCacheStatistics);
        }catch(Exception e){
            log.warn("Failed to register ChronoGraph MBeans. JMX functionality will not be available for this instance. Exception is: " + e);
        }
    }

}
