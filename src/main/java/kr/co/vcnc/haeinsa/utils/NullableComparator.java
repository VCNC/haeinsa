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
package kr.co.vcnc.haeinsa.utils;

import java.util.Comparator;

/**
 * Wrapper of Comparator. It allow to compare element with null. Null is assumed
 * to be the smallest element.
 *
 * @param <T> the type of element to compare with
 */
public class NullableComparator<T> implements Comparator<T> {
    private final Comparator<T> comparator;

    public NullableComparator(Comparator<T> comparator) {
        this.comparator = comparator;
    }

    @Override
    public int compare(T o1, T o2) {
        if (o1 == null && o2 != null) {
            return -1;
        } else if (o1 != null && o2 != null) {
            return comparator.compare(o1, o2);
        } else if (o1 != null && o2 == null) {
            return 1;
        }
        return 0;
    }
}
