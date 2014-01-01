/**
 * Copyright (C) 2014 VCNC Inc.
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

import java.io.Closeable;
import java.io.IOException;

/**
 * Scanner wrapper of HaeinsaResult. Can contain multiple HaeinsaResult inside
 * to allow iterator pattern.
 * <p>
 * HaeinsaResultScanner interface provides both iterator pattern and next(),
 * next(int) methods.
 */
public interface HaeinsaResultScanner extends Closeable, Iterable<HaeinsaResult> {

    /**
     * Grab the next row's worth of values. The scanner will return a Result.
     *
     * @return Result object if there is another row, null if the scanner is
     *         exhausted.
     * @throws IOException e
     */
    HaeinsaResult next() throws IOException;

    /**
     * @param nbRows number of rows to return
     * @return Between zero and nbRows Results
     * @throws IOException e
     */
    HaeinsaResult[] next(int nbRows) throws IOException;

    /**
     * Closes the scanner and releases any resources it has allocated
     */
    @Override
    void close();

}
