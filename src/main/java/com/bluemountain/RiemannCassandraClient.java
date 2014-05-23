package com.bluemountain;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Event.Builder;
import com.aphyr.riemann.client.RiemannClient;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.MemoryUsage;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Monitoring client that polls Cassandra MBeans for metrics and streams them to
 * riemann as events
 *
 * @author jluciani
 *
 */
public class RiemannCassandraClient
{
    private static Options options = null;
    final RiemannClient riemannClient;

    static
    {
        options = new Options();

        options.addOption("riemann_host", true, "hostname for riemann server");
        options.addOption("riemann_port", true, "port number for riemann server");
        options.addOption("cassandra_host", true, "hostname for cassandra node");
        options.addOption("jmx_port", true, "port number for jmx on cassandra node");
        options.addOption("jmx_username", true, "username for cassandra jmx agent");
        options.addOption("jmx_password", true, "password cassandra jmx agent");
        options.addOption("interval_seconds", true, "number of seconds between updates");
    }

    volatile NodeProbe jmxClient = null;
    final String cassandraHost;
    final Integer cassandraJmxPort;
    final String jmxUsername;
    final String jmxPassword;
    final Proto.Event protoEvent;

    public RiemannCassandraClient(String riemannHost, Integer riemannPort, String cassandraHost, Integer cassandraJmxPort, String jmxUsername, String jmxPassword)
    {
        this.cassandraHost = cassandraHost;
        this.cassandraJmxPort = cassandraJmxPort;
        this.jmxUsername = jmxUsername;
        this.jmxPassword = jmxPassword;

        this.protoEvent = Proto.Event.newBuilder().setHost(pickBestHostname(cassandraHost)).addTags("cassandra").setState("ok").setTtl(5.0F).build();

        this.riemannClient = new RiemannClient(new InetSocketAddress(riemannHost, riemannPort.intValue()));
        if (!reconnectJMX()) {
            System.err.println(String.format("Unable to connect to Cassandra JMX (%s:%d) will continue to try silently....", new Object[] { cassandraHost, cassandraJmxPort }));
        }
    }

    private String pickBestHostname(String cassandraHost)
    {
        try
        {
            InetAddress cassandraAddr = InetAddress.getByName(cassandraHost);
            if (!cassandraAddr.isLoopbackAddress()) {
                return cassandraAddr.getCanonicalHostName();
            }
            //Pick first non local ip with a hostname
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets))
            {
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress inetAddress : Collections.list(inetAddresses)) {
                    if ((!inetAddress.isLoopbackAddress()) && ((inetAddress instanceof Inet4Address))) {
                        return inetAddress.getCanonicalHostName();
                    }
                }
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Unknown host", e);
        }
        catch (SocketException e)
        {
            throw new RuntimeException("Error getting network info", e);
        }
        return cassandraHost;
    }

    private synchronized boolean reconnectJMX()
    {
        try
        {
            if (this.jmxUsername == null) {
                this.jmxClient = new NodeProbe(this.cassandraHost, this.cassandraJmxPort.intValue());
            } else {
                this.jmxClient = new NodeProbe(this.cassandraHost, this.cassandraJmxPort.intValue(), this.jmxUsername, this.jmxPassword);
            }
            return true;
        }
        catch (Exception e)
        {
            // We silently continue
            this.jmxClient = null;
        }
        return false;
    }

    private void add(List<Proto.Event> events, String name, float val)
    {
        events.add(Proto.Event.newBuilder(this.protoEvent).setService(name).setMetricF(val).build());
    }

    private void add(List<Proto.Event> events, String name, float val, String desc)
    {
        events.add(Proto.Event.newBuilder(this.protoEvent).setService(name).setMetricF(val).setDescription(desc).build());
    }

    private long emitColumnFamilyMetrics()
    {
        List<Proto.Event> events = new ArrayList();

        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> it = this.jmxClient.getColumnFamilyStoreMBeanProxies();


        // CF metrics
        long totalBytes = 0L;
        while (it.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> e = (Map.Entry)it.next();

            String name = "cassandra.db." + (String)e.getKey() + "." + ((ColumnFamilyStoreMBean)e.getValue()).getColumnFamilyName();
            ColumnFamilyStoreMBean v = (ColumnFamilyStoreMBean)e.getValue();

            add(events, name + ".keys", (float)(v.estimateKeys() / 1000L));
            add(events, name + ".total_sstable_mb", (float)(v.getLiveDiskSpaceUsed() / 1048576L));
            add(events, name + ".total_bloom_mb", (float)(v.getBloomFilterDiskSpaceUsed() / 1048576L));
            add(events, name + ".bloom_fp_rate", (float)v.getRecentBloomFilterFalseRatio());
            add(events, name + ".max_row_size_kb", (float)(v.getMaxRowSize() / 1024L));
            add(events, name + ".min_row_size_kb", (float)(v.getMinRowSize() / 1024L));
            add(events, name + ".mean_row_size_kb", (float)(v.getMeanRowSize() / 1024L));
            add(events, name + ".sstable_count", v.getLiveSSTableCount());
            add(events, name + ".memtable_size_mb", (float)v.getMemtableDataSize() / 1048576.0F);


            // latencies can return NaN
            Float f = Float.valueOf((float)v.getRecentReadLatencyMicros());
            add(events, name + ".read_latency", f.equals(Float.NaN) ? 0.0F : f.floatValue());

            f = Float.valueOf((float)((ColumnFamilyStoreMBean)e.getValue()).getRecentWriteLatencyMicros());
            add(events, name + ".write_latency", f.equals(Float.NaN) ? 0.0F : f.floatValue());

            totalBytes += ((ColumnFamilyStoreMBean)e.getValue()).getLiveDiskSpaceUsed();

            this.riemannClient.sendEvents((Proto.Event[])events.toArray(new Proto.Event[0]));
            events.clear();
        }
        return totalBytes;
    }

    private void emitThreadPoolMetrics()
    {
        List<Proto.Event> events = new ArrayList();

        Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> it = this.jmxClient.getThreadPoolMBeanProxies();
        while (it.hasNext())
        {
            Map.Entry<String, JMXEnabledThreadPoolExecutorMBean> p = (Map.Entry)it.next();

            String name = "cassandra.tp." + (String)p.getKey();
            JMXEnabledThreadPoolExecutorMBean v = (JMXEnabledThreadPoolExecutorMBean)p.getValue();

            add(events, name + ".completed", (float)v.getCompletedTasks());
            add(events, name + ".active", v.getActiveCount());
            add(events, name + ".pending", (float)v.getPendingTasks());
            add(events, name + ".blocked", v.getCurrentlyBlockedTasks());

            this.riemannClient.sendEvents((Proto.Event[])events.toArray(new Proto.Event[0]));
            events.clear();
        }
    }

    private void emitDroppedMessagesMetrics() {
        List<Event> events = new ArrayList<Event>();
        Map<String, Integer> dropped = jmxClient.getDroppedMessages();
        for (Map.Entry<String, Integer> me: dropped.entrySet()) {
            add(events, "org.apache.cassandra.metrics.dropped."+me.getKey(), (float) me.getValue());
        }
        riemannClient.sendEvents(events.toArray(new Event[] {}));
        events.clear();
    }

    private void emitMetrics()
    {
        if ((this.jmxClient == null) && (!reconnectJMX())) {
            return;
        }
        try
        {
            // TP Metrics
            emitThreadPoolMetrics();


            // CF Metrics
            long totalSSTableBytes = emitColumnFamilyMetrics();

            // Dropped messages metrics
            emitDroppedMessagesMetrics();

            // Basic metrics
            List<Proto.Event> events = new ArrayList();

            add(events, "cassandra.exception_count", this.jmxClient.getExceptionCount());
            add(events, "cassandra.heap_used_mb", (float)(this.jmxClient.getHeapMemoryUsage().getUsed() / 1048576L));
            add(events, "cassandra.heap_max_mb", (float)(this.jmxClient.getHeapMemoryUsage().getMax() / 1048576L));
            add(events, "cassandra.heap_committed_mb", (float)(this.jmxClient.getHeapMemoryUsage().getCommitted() / 1048576L));
            add(events, "cassandra.recent_timeouts", (float)this.jmxClient.msProxy.getRecentTotalTimouts(), FBUtilities.json(this.jmxClient.msProxy.getRecentTimeoutsPerHost()));
            add(events, "cassandra.pending_compactions", this.jmxClient.getCompactionManagerProxy().getPendingTasks());
            add(events, "cassandra.total_sstable_mb", (float)(totalSSTableBytes / 1048576L));

            this.riemannClient.sendEvents((Proto.Event[])events.toArray(new Proto.Event[0]));
        }
        catch (Exception t)
        {
            // Try again later
            this.jmxClient = null;
            t.printStackTrace();
        }
    }

    public static void printUsage()
    {
        PrintWriter writer = new PrintWriter(System.out);
        HelpFormatter usageFormatter = new HelpFormatter();
        usageFormatter.printUsage(writer, 80, "riemann-cassandra", options);
        writer.close();
    }

    public static void main(String[] args)
    {
        BasicParser parser = new BasicParser();
        CommandLine cl = null;
        try
        {
            cl = parser.parse(options, args);
        }
        catch (ParseException e)
        {
            printUsage();
            System.exit(1);
        }
        // Extracted options
        String cassandraHost = cl.getOptionValue("cassandra_host", "localhost");
        String riemannHost = cl.getOptionValue("riemann_host", "localhost");
        String jmxUsername = cl.getOptionValue("jmx_username");
        String jmxPassword = cl.getOptionValue("jmx_password");
        Integer jmxPort = Integer.valueOf(cl.getOptionValue("jmx_port", "7199"));
        Integer riemannPort = Integer.valueOf(cl.getOptionValue("riemann_port", "5555"));
        Integer intervalSeconds = Integer.valueOf(cl.getOptionValue("interval_seconds", "5"));

        RiemannCassandraClient cli = new RiemannCassandraClient(riemannHost, riemannPort, cassandraHost, jmxPort, jmxUsername, jmxPassword);
        for (;;)
        {
            cli.emitMetrics();
            try
            {
                Thread.sleep(intervalSeconds.intValue() * 1000);
            }
            catch (InterruptedException e) {}
        }
    }
}
