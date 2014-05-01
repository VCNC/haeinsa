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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

public abstract class HaeinsaQuery extends HaeinsaOperation {
    protected Filter filter = null;
    protected boolean cacheBlocks = true;

    /**
     * Get server-side filter instance of this operation.
     *
     * @return Filter instance of this operation
     */
    public Filter getFilter() {
        return filter;
    }

    /**
     * Apply the specified server-side filter when performing this operation.
     * Only {@link Filter#filterKeyValue(KeyValue)} is called AFTER all tests
     * for ttl, column match, deletes and max versions have been run.
     *
     * @param filter filter to run on the server
     * @return this for invocation chaining
     */
    public HaeinsaQuery setFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Get whether filter has been specified for this operation.
     *
     * @return true is a filter has been specified, false if not
     */
    public boolean hasFilter() {
        return filter != null;
    }

    /**
     * Set whether blocks should be cached for this operation.
     * Generally caching block help next get/scan requests to the same block,
     * but DB consume more memory which could cause longer jvm gc or cache churn.
     * CacheBlocks and caching are different configurations.
     * <p>
     * This is true by default. When true, default settings of the table and
     * family are used (this will never override caching blocks if the block
     * cache is disabled for that family or entirely).
     *
     * @param cacheBlocks if false, default settings are overridden and blocks
     *        will not be cached
     */
    public void setCacheBlocks(boolean cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }

    /**
     * Get whether blocks should be cached for this operation.
     *
     * @return true if default setting of block caching should be used, false if
     *         blocks should not be cached
     */
    public boolean getCacheBlocks() {
        return cacheBlocks;
    }
}
