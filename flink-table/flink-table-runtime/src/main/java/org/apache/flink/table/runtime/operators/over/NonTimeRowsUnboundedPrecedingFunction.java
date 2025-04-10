/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The NonTimeRowsUnboundedPrecedingFunction class is a specialized implementation for processing
 * unbounded OVER window aggregations, particularly for non-time-based rows queries in Apache Flink.
 * It maintains strict ordering of rows within partitions and handles the full changelog lifecycle
 * (inserts, updates, deletes).
 *
 * <p>Key Components and Assumptions
 *
 * <p>Data Structure Design: (1) Maintains a sorted list of tuples containing sort keys and lists of
 * IDs for each key (2) Each incoming row is assigned a unique Long ID (starting from
 * Long.MIN_VALUE) (3) Uses multiple state types to track rows, sort orders, and aggregations
 *
 * <p>State Management: (1) idState: Counter for generating unique row IDs (2) sortedListState:
 * Ordered list of sort keys with their associated row IDs (3) valueMapState: Maps IDs to their
 * corresponding input rows (4) accMapState: Maps IDs to their accumulated values
 *
 * <p>Processing Model: (1) For inserts/updates: Adds rows to the appropriate position based on sort
 * key (2) For deletes: Removes rows by matching both sort key and row content (3) Recalculates
 * aggregates for affected rows and emits the appropriate events (4) Skips redundant events when
 * accumulators haven't changed to reduce network traffic
 *
 * <p>Optimization Assumptions: (1) Skip emitting updates when accumulators haven't changed to
 * reduce network traffic (2) Uses state TTL for automatic cleanup of stale data (3) Carefully
 * manages row state to support incremental calculations
 *
 * <p>Retraction Handling: (1) Handles retraction mode (DELETE/UPDATE_BEFORE) events properly (2)
 * Supports the proper processing of changelog streams
 *
 * <p>Limitations
 *
 * <p>Linear search performance: - The current implementation uses a linear search to find the
 * correct position for each sort key. This can be optimized using a binary search for large state
 * sizes.
 *
 * <p>State size and performance: - The implementation maintains multiple state types that could
 * grow large with high cardinality data
 *
 * <p>Linear recalculation: - When processing updates, all subsequent elements need to be
 * recalculated, which could be inefficient for large windows
 */
public class NonTimeRowsUnboundedPrecedingFunction<K>
        extends AbstractNonTimeUnboundedPrecedingOver<K> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(NonTimeRowsUnboundedPrecedingFunction.class);

    public NonTimeRowsUnboundedPrecedingFunction(
            long stateRetentionTime,
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            GeneratedRecordEqualiser genSortKeyEqualiser,
            GeneratedRecordComparator genSortKeyComparator,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            LogicalType[] sortKeyTypes,
            RowDataKeySelector sortKeySelector,
            InternalTypeInfo<RowData> accKeyRowTypeInfo) {
        super(
                stateRetentionTime,
                genAggsHandler,
                genRecordEqualiser,
                genSortKeyEqualiser,
                genSortKeyComparator,
                accTypes,
                inputFieldTypes,
                sortKeyTypes,
                sortKeySelector,
                accKeyRowTypeInfo);
    }

    /**
     * Adds a new element(insRow) to a sortedList. The sortedList contains a list of Tuple(SortKey,
     * List[Long Ids]>). Extracts the inputSortKey from the insRow and compares it with every
     * element in the sortedList. If a sortKey already exists in the sortedList for the input, add
     * the id to the list of ids and update the sortedList, otherwise find the right position in the
     * sortedList and add a new entry in the sortedList. After the insRow is successfully inserted,
     * an INSERT/UPDATE_AFTER event is emitted for the newly inserted element, and for all
     * subsequent elements an UPDATE_BEFORE and UPDATE_AFTER event is emitted based on the previous
     * and newly aggregated values. Some updates are skipped if the previously accumulated value is
     * the same as the newly accumulated value to save on network bandwidth and downstream
     * processing including writing the result to the sink system.
     *
     * @param insRow The input value.
     * @param out The collector for returning result values.
     * @throws Exception
     */
    protected void insertIntoSortedList(RowData insRow, Collector<RowData> out) throws Exception {
        Long id = getNextId();
        List<Tuple2<RowData, List<Long>>> sortedList = getSortedList();
        RowKind origRowKind = insRow.getRowKind();
        insRow.setRowKind(RowKind.INSERT);
        RowData inputSortKey = sortKeySelector.getKey(insRow);
        Tuple2<Integer, Boolean> indexForInsertOrUpdate =
                findIndexOfSortKey(sortedList, inputSortKey, false);
        boolean isInsert = indexForInsertOrUpdate.f1;
        int index = indexForInsertOrUpdate.f0;
        if (isInsert) {
            if (index == -1) {
                // Insert at the end of the sortedList
                sortedList.add(new Tuple2<>(inputSortKey, List.of(id)));
                index = sortedList.size() - 1;
            } else {
                // Insert at position i of the sortedList
                sortedList.add(index, new Tuple2<>(inputSortKey, List.of(id)));
            }
            setAccumulatorOfPrevId(sortedList, index - 1, -1);
            aggFuncs.accumulate(insRow);
            collectInsertOrUpdateAfter(out, insRow, origRowKind, aggFuncs.getValue());
        } else {
            // Update at position specified by index
            List<Long> ids = new ArrayList<>(sortedList.get(index).f1);
            ids.add(id);
            sortedList.set(index, new Tuple2<>(inputSortKey, ids));
            setAccumulatorOfPrevId(sortedList, index, ids.size() - 2);
            reAccumulateIdsAfterInsert(aggFuncs.getAccumulators(), ids, insRow);
            emitUpdatesForIds(
                    ids,
                    ids.size() - 1,
                    accMapState.get(GenericRowData.of(ids.get(ids.size() - 2))), // prevAcc
                    aggFuncs.getAccumulators(), // currAcc
                    origRowKind,
                    insRow,
                    out);
        }

        // Add/Update state
        valueMapState.put(id, insRow);
        accMapState.put(GenericRowData.of(id), aggFuncs.getAccumulators());
        sortedListState.update(sortedList);
        idState.update(++id);

        processRemainingElements(sortedList, index + 1, aggFuncs.getAccumulators(), out);
    }

    /**
     * Helper method to re-accumulate the aggregated value for all ids after an id was inserted to
     * the end of the ids list.
     *
     * @param currAcc
     * @param ids
     * @param insRow
     * @throws Exception
     */
    void reAccumulateIdsAfterInsert(RowData currAcc, List<Long> ids, RowData insRow)
            throws Exception {
        aggFuncs.setAccumulators(currAcc);
        // For rows, there is no need to re-accumulate all ids for the same sort key,
        // since every id will have a unique aggregated value
        // Update acc for newly inserted row
        aggFuncs.accumulate(insRow);
    }

    /**
     * Helper method to send updates for ids. To comply with sql rows syntax, only send update for
     * the changed row.
     *
     * @param ids
     * @param idxOfChangedRow
     * @param out
     * @param rowKind
     * @param changedRow
     * @param prevAggValue
     * @param currAggValue
     */
    void sendUpdatesForIds(
            List<Long> ids,
            int idxOfChangedRow,
            Collector<RowData> out,
            RowKind rowKind,
            RowData changedRow,
            RowData prevAggValue,
            RowData currAggValue) {
        for (int j = 0; j < ids.size(); j++) {
            if (j == idxOfChangedRow) {
                sendUpdateForChangedRow(out, rowKind, changedRow, prevAggValue, currAggValue);
                break;
            }
        }
    }

    /**
     * Process all sort keys starting from startPos location. Calculates the new agg value for all
     * ids and emits the old and new aggregated values for the ids if the newly aggregated value is
     * different from the previously aggregated values.
     *
     * @param sortedList
     * @param startPos
     * @param currAcc
     * @param out
     * @throws Exception
     */
    protected void processRemainingElements(
            List<Tuple2<RowData, List<Long>>> sortedList,
            int startPos,
            RowData currAcc,
            Collector<RowData> out)
            throws Exception {
        aggFuncs.setAccumulators(currAcc);
        // Send updates for remaining sort keys after the inserted/removed idx
        for (int i = startPos; i < sortedList.size(); i++) {
            Tuple2<RowData, List<Long>> sortKeyAndIds = sortedList.get(i);
            List<Long> ids = sortKeyAndIds.f1;

            // Calculate and update new agg value for all ids
            // Note: Each id will have a unique aggregated value even for ids with the same sort key
            // to comply with the sql rows syntax
            for (int j = 0; j < ids.size(); j++) {
                RowData value = valueMapState.get(ids.get(j));
                aggFuncs.accumulate(value);
                // Logic to early out
                // TODO: Move comparison to function i.e. canEarlyOut(prev, curr)
                if (aggFuncs.getValue().equals(accMapState.get(GenericRowData.of(ids.get(j))))) {
                    // Previous accumulator is the same as the current accumulator.
                    // This means all the ids will have no change in the accumulated value.
                    // Skip sending downstream updates in such cases to reduce network traffic
                    LOG.debug(
                            "Prev accumulator is same as curr accumulator. Skipping further updates.");
                    return;
                }
                collectUpdateBefore(out, value, accMapState.get(GenericRowData.of(ids.get(j))));
                collectUpdateAfter(out, value, aggFuncs.getValue());
                accMapState.put(GenericRowData.of(ids.get(j)), aggFuncs.getValue());
            }
        }
    }

    /**
     * Removes the row matching delRow from the sortedList. Matching is done based on the sortKeys.
     * Once matched, the actual row is compared for all ids belonging to the same sortKey. After the
     * element is removed, a DELETE event is emitted for the removed element, and for all subsequent
     * elements an UPDATE_BEFORE and UPDATE_AFTER event is emitted based on the previous and newly
     * aggregated values. Updates are skipped if the previously accumulated value is the same as the
     * newly accumulated value to save on network bandwidth.
     *
     * @param delRow
     * @param out
     * @throws Exception
     */
    void removeFromSortedList(RowData delRow, Collector<RowData> out) throws Exception {
        delRow.setRowKind(RowKind.INSERT);
        RowData inputSortKey = sortKeySelector.getKey(delRow);
        List<Tuple2<RowData, List<Long>>> sortedList = getSortedList();

        int idxOfSortKey = findIndexOfSortKey(sortedList, inputSortKey, true).f0;
        if (idxOfSortKey == -1) {
            LOG.debug("Could not find matching sort key. Skipping delete.");
            numOfSortKeysNotFound.inc();
            return;
        }

        RowData curSortKey = sortedList.get(idxOfSortKey).f0;
        List<Long> ids = new ArrayList<>(sortedList.get(idxOfSortKey).f1);

        int removeIndex = findIndexOfIdToBeRemoved(ids, delRow);
        if (removeIndex == -1) {
            LOG.info("Could not find matching row to remove. Missing id from sortKey ids list.");
            numOfIdsNotFound.inc();
            return;
        }

        // Get previous acc
        final RowData prevAcc = getPreviousAccumulator(sortedList, idxOfSortKey, removeIndex);
        if (prevAcc == null) {
            aggFuncs.createAccumulators();
        } else {
            aggFuncs.setAccumulators(prevAcc);
        }

        // Re-accumulate ids and emit updates for ids
        reAccumulateIdsAndEmitUpdates(ids, removeIndex, out);

        // Remove id and update sortedList
        Long deletedId = ids.remove(removeIndex);
        idxOfSortKey = removeIdFromSortedList(sortedList, idxOfSortKey, ids, curSortKey);

        // Update state
        valueMapState.remove(deletedId);
        accMapState.remove(GenericRowData.of(deletedId));
        sortedListState.update(sortedList);

        processRemainingElements(sortedList, idxOfSortKey, aggFuncs.getAccumulators(), out);
    }
}
