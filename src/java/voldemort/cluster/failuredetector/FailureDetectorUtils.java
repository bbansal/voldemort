/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort.cluster.failuredetector;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import voldemort.utils.JmxUtils;
import voldemort.utils.ReflectUtils;

/**
 * FailureDetectorUtils serves as a factory for creating a FailureDetector
 * implementation.
 * 
 * @author Kirk True
 */

public class FailureDetectorUtils {

    public static FailureDetector create(FailureDetectorConfig failureDetectorConfig) {
        Class<?> clazz = ReflectUtils.loadClass(failureDetectorConfig.getImplementationClassName());
        FailureDetector fd = (FailureDetector) ReflectUtils.callConstructor(clazz,
                                                                            new Class[] { FailureDetectorConfig.class },
                                                                            new Object[] { failureDetectorConfig });

        if(failureDetectorConfig.isJmxEnabled()) {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = JmxUtils.createObjectName(JmxUtils.getPackageName(fd.getClass()),
                                                        fd.getClass().getSimpleName());

            if(mbeanServer.isRegistered(name))
                JmxUtils.unregisterMbean(mbeanServer, name);

            JmxUtils.registerMbean(mbeanServer, JmxUtils.createModelMBean(fd), name);
        }

        return fd;
    }

    public static FailureDetector create(FailureDetectorConfig failureDetectorConfig,
                                         FailureDetectorListener... failureDetectorListener) {
        FailureDetector fd = create(failureDetectorConfig);

        if(failureDetectorListener != null) {
            for(FailureDetectorListener fdl: failureDetectorListener)
                fd.addFailureDetectorListener(fdl);
        }

        return fd;
    }

}
