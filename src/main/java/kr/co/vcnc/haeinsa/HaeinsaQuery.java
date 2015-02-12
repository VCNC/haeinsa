/**
 * Copyright (C) 2013-2015 VCNC Inc.
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

public abstract class HaeinsaQuery extends HaeinsaOperation {
    protected boolean cacheBlocks = true;

    /**
     * Set whether blocks should be cached for this Scan.
     * Generally caching block help next get/scan requests to the same block,
     * but DB consume more memory which could cause longer jvm gc or cache churn.
     * CacheBlocks and caching are different configurations.
     * <p>
     * This is true by default. When true, default settings of the table and
     * family are used (this will never override caching blocks if the block
     * cache is disabled for that family or entirely).
     *
     * @param cacheBlocks if false, default settings are overridden and blocks
     * will not be cached
     */
    public void setCacheBlocks(boolean cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }

    /**
     * Get whether blocks should be cached for this Scan.
     *
     * @return true if default setting of block caching should be used, false if
     * blocks should not be cached
     */
    public boolean getCacheBlocks() {
        return cacheBlocks;
    }
}
