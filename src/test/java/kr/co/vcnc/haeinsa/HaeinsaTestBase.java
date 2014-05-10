/**
 * Copyright (C) 2013-2014 VCNC Inc.
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
import org.hsqldb.TransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Basic test class for Haeinsa unit tests.
 * This class automatically create CLUSTER, and table name of each test method.
 * You can get context of the test with {@link #context()}.
 */
public class HaeinsaTestBase {
    private static HaeinsaTestingCluster CLUSTER;
    private static final ThreadLocal<TestingContext> CONTEXT = new ThreadLocal<>();

    @BeforeClass
    public static void setUpHbase() throws Exception {
        CLUSTER = HaeinsaTestingCluster.getInstance();
    }

    @BeforeMethod
    public void generateTableName(Method method) {
        final String className = method.getDeclaringClass().getName();
        final String methodName = method.getName();
        CONTEXT.set(new TestingContext(className, methodName));
    }

    @AfterMethod
    public void releaseTableName() {
        CONTEXT.remove();
    }

    public static TestingContext context() {
        return CONTEXT.get();
    }

    /**
     * Context of current test. You can create {@link HTableInterface} and {@link HaeinsaTableIface}
     * with this class. You can also get instance of {@link TransactionManager} with
     * {@link #getTransactionManager()}.
     */
    public static final class TestingContext {
        private final String className;
        private final String methodName;

        private TestingContext(String className, String methodName) {
            this.className = className;
            this.methodName = methodName;
        }

        /**
         * Class name of the current test.
         *
         * @return class name of the current test
         */
        public String getClassName() {
            return className;
        }

        /**
         * Method name of the current test.
         *
         * @return method name of the current test
         */
        public String getMethodName() {
            return methodName;
        }

        /**
         * Get {@link HaeinsaTestingCluster} instance for test.
         * Since creating new testing cluster takes long time, each test case reuse the cluster.
         * For this reason, the testing cluster is managed as singleton,
         * so this method always returns same instance of {@link HaeinsaTestingCluster}.
         *
         * @return instance of {@link HaeinsaTestingCluster}
         */
        public HaeinsaTestingCluster getCluster() {
            return CLUSTER;
        }

        /**
         * Get {@link HaeinsaTransactionManager} instance for test.
         *
         * @return instance of {@link HaeinsaTransactionManager}
         */
        public HaeinsaTransactionManager getTransactionManager() {
            return CLUSTER.getTransactionManager();
        }

        /**
         * Create {@link HaeinsaTableIface} with table name.
         * The name of the table will be created with {@link #createContextedTableName(String)}.
         *
         * @param tableName table name of the HaeinsaTable
         * @return instance of {@link HaeinsaTableIface}
         * @throws Exception if there is a problem instantiating the HaeinsaTable
         */
        public HaeinsaTableIface getHaeinsaTableIface(String tableName) throws Exception {
            return getCluster().getHaeinsaTable(createContextedTableName(tableName));
        }

        /**
         * Create {@link HTableInterface} with table name.
         * The name of the table will be created with {@link #createContextedTableName(String)}.
         *
         * @param tableName table name of the HTable
         * @return instance of {@link HTableInterface}
         * @throws Exception if there is a problem instantiating the HTable
         */
        public HTableInterface getHTableInterface(String tableName) throws Exception {
            return getCluster().getHbaseTable(createContextedTableName(tableName));
        }

        /**
         * Create table name for current test method.
         * This method create table name with class name and method name of the test.
         * Since, tests in Haeinsa reuse {@link HaeinsaTestingCluster}, data in the cluster would
         * affect to each test cases. To avoid this, each test should use it's own table.
         * This is why this method is needed.
         *
         * @param tableName table name
         * @return prefixed table name with class and method name of the test
         */
        public String createContextedTableName(String tableName) {
            return String.format("%s.%s.%s", className, methodName, tableName);
        }
    }
}
