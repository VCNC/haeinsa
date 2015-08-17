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

/**
 * Created by ehud on 8/17/15.
 * We might want to consider optimize the addMutation and use java.util.concurrent list such as CopyOnWriteArrayList
 */
class HaeinsaRowTransactionThreadSafe extends HaeinsaRowTransaction {

    HaeinsaRowTransactionThreadSafe(HaeinsaTableTransaction tableTransaction) {
        super(tableTransaction);
    }

    @Override
    public synchronized void addMutation(HaeinsaMutation mutation) {
        super.addMutation(mutation);
    }

}
