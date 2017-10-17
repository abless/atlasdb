/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsColumn;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsColumnValue;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsRow;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public final class DynamicColumnsRangeScanBenchmark extends AbstractRangeScanBenchmark {

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient, int numRows, int dataSize, int numUpdates, int numDeleted) {
        return new DynamicColumnsRangeScanBenchmark(txnManager, numClients, requestsPerClient, numRows,
                dataSize, numUpdates, numDeleted).execute();
    }

    private DynamicColumnsRangeScanBenchmark(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient,
            int numRows, int dataSize, int numUpdates, int numDeleted) {
        super(numClients, requestsPerClient, txnManager, numRows, dataSize, numUpdates, numDeleted);
    }

    @Override
    protected void writeValues(Transaction txn, Map<Long, byte[]> valuesByKey) {
        KvDynamicColumnsTable table = BenchmarksTableFactory.of().getKvDynamicColumnsTable(txn);

        valuesByKey.forEach((key, value) -> {
            table.put(
                    KvDynamicColumnsRow.of(bucket),
                    KvDynamicColumnsColumnValue.of(KvDynamicColumnsColumn.of(key), value));
        });
    }

    @Override
    protected void deleteValues(Transaction txn, Set<Long> keys) {
        KvDynamicColumnsTable table = BenchmarksTableFactory.of().getKvDynamicColumnsTable(txn);

        keys.forEach(key -> {
            table.delete(KvDynamicColumnsRow.of(bucket),
                    KvDynamicColumnsColumn.of(key));
        });
    }

    @Override
    protected long getRange(Transaction txn, long startInclusive, long endExclusive) {
        KvDynamicColumnsTable table = BenchmarksTableFactory.of().getKvDynamicColumnsTable(txn);

        List<byte[]> data = Lists.newArrayList();
        Iterator iterator = table.getRowsColumnRange(
                ImmutableSet.of(KvDynamicColumnsRow.of(bucket)),
                new ColumnRangeSelection(
                        KvDynamicColumnsColumn.of(startInclusive).persistToBytes(),
                        KvDynamicColumnsColumn.of(endExclusive).persistToBytes()),
                batchSize);

        return Iterators.size(iterator);
    }


}