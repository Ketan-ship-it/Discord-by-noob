package gateway.Metrics;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

public class MetricsCollector {

    public static long getDirectMemoryUsage() {

        for (BufferPoolMXBean pool :
                ManagementFactory.getPlatformMXBeans(
                        BufferPoolMXBean.class)) {

            if ("direct".equals(pool.getName())) {
                return pool.getMemoryUsed();
            }
        }

        return 0;
    }
}