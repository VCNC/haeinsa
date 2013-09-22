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
namespace java kr.co.vcnc.haeinsa.thrift.generated

enum TRowLockState {
    STABLE = 1,
    PREWRITTEN = 2,
    ABORTED = 3,
    COMMITTED = 4,
}

struct TRowKey {
    1: required binary tableName,
    2: required binary row,
}

struct TCellKey {
    1: required binary family,
    2: required binary qualifier,
}

enum TMutationType {
    PUT = 1,
    REMOVE = 2,
}

struct TKeyValue {
    1: required TCellKey key,
    2: required binary value,
}

struct TPut {
    1: required list<TKeyValue> values;
}

struct TRemove {
    1: optional list<TCellKey> removeCells;
    2: optional list<binary> removeFamilies;
}

struct TMutation {
    1: required TMutationType type,
    2: optional TPut put,
    3: optional TRemove remove,
}

struct TRowLock {
    1: required i32 version,
    2: required TRowLockState state,
    3: required i64 commitTimestamp,
    4: optional i64 currentTimestmap,
    5: optional i64 expiry,
    6: optional TRowKey primary,
    7: optional list<TRowKey> secondaries,
    8: optional list<TCellKey> prewritten,
    9: optional list<TMutation> mutations,
    10: optional i64 prewriteTimestamp,
}