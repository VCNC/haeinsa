/**
 * Copyright (C) 2013 VCNC, inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kr.co.vcnc.haeinsa;

import java.lang.reflect.Method;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Basic test class for Haeinsa unit tests.
 * This class automaticall create CLUSTER, and table name of each test method.
 * You can use CLUSTER to get {@link HaeinsaTableIface} and {@link HTableInterface}.
 * You can also use {@link #currentTestName() currentTableName} to get current table name for test
 */
public class HaeinsaTestBase {
    protected static HaeinsaTestingCluster CLUSTER;

    protected final ThreadLocal<String> currentTableName = new ThreadLocal<>();

    @BeforeClass
    public static void setUpHbase() throws Exception {
        CLUSTER = HaeinsaTestingCluster.getInstance();
    }

    @BeforeMethod
    public void generateTableName(Method method) {
        final String className = method.getDeclaringClass().getName();
        final String methodName = method.getName();
        currentTableName.set(String.format("%s.%s", className, methodName));
    }

    @AfterMethod
    public void releaseTableName() {
        currentTableName.remove();
    }

    /**
     * Get table name for current test method.
     * Table name is automatically generate before execution of the test method.
     * This method construct with name of the test class and name of the test method.
     * So, this table name is unique for specific test.
     * @return table name for current test method
     */
    public String currentTableName() {
        return currentTableName.get();
    }
}
