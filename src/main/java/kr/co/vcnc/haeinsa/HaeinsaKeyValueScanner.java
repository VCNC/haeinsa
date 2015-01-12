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

import java.io.IOException;
import java.util.Comparator;

import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import com.google.common.collect.ComparisonChain;

/**
 * Scanner wrapper of HaeinsaKeyValue. Contains multiple HaeinsaKeyValue inside
 * to allow iterator pattern.
 * <p>
 * HaeinsaKeyValueScanner interface provides additional
 * {@link HaeinsaKeyValueScanner#peek} method to peek element of scanner without
 * moving iterator.
 * <p>
 * Each HaeinsaKeyValueScanner have sequenceId which represent which scanner is
 * newer one.
 */
public interface HaeinsaKeyValueScanner {
    Comparator<HaeinsaKeyValueScanner> COMPARATOR = new Comparator<HaeinsaKeyValueScanner>() {

        @Override
        public int compare(HaeinsaKeyValueScanner o1, HaeinsaKeyValueScanner o2) {
            return ComparisonChain.start()
                    .compare(o1.peek(), o2.peek(), HaeinsaKeyValue.COMPARATOR)
                    .compare(o1.getSequenceID(), o2.getSequenceID())
                    .result();
        }
    };

    /**
     * Look at the next KeyValue in this scanner, but do not iterate scanner.
     *
     * @return the next KeyValue
     */
    HaeinsaKeyValue peek();

    /**
     * Return the next KeyValue in this scanner, iterating the scanner
     *
     * @return the next KeyValue
     */
    HaeinsaKeyValue next() throws IOException;

    /**
     *
     * @return Return TRowLock if exist in HaeinsaKeyValue. Otherwise, return null
     * @throws IOException
     */
    TRowLock peekLock() throws IOException;

    /**
     * Get the sequence id associated with this KeyValueScanner. This is
     * required for comparing multiple KeyValueScanners to find out which one
     * has the latest data. The default implementation for this would be to
     * return 0. A KeyValueScanner having lower sequence id will be considered
     * to be the newer one.
     */
    long getSequenceID();

    /**
     * Close the KeyValue scanner.
     */
    void close();
}
