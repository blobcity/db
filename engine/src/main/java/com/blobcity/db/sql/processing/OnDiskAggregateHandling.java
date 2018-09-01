/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.db.sql.processing;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.foundationdb.sql.parser.AggregateNode;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Handles disk based aggregate operations. Supports COUNT(*), COUNT(col), SUM, AVG, MIN MAX
 *
 * @author sanketsarang
 */
@Component
public class OnDiskAggregateHandling {

    @Autowired @Lazy
    private OnDiskCountHandling countHandling;
    @Autowired @Lazy
    private OnDiskSumHandling sumHandling;
    @Autowired @Lazy
    private OnDiskAvgHandling avgHandling;
    @Autowired @Lazy
    private OnDiskMaxHandling maxHandling;
    @Autowired @Lazy
    private OnDiskMinHandling minHandling;


    public Object computeAgg(final String ds, final String collection, final AggregateNode aggregateNode) throws OperationException {
        if("count(*)".equalsIgnoreCase(aggregateNode.getAggregateName())
                || "count".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            return countHandling.computeCount(ds, collection, aggregateNode);

        } else if("max".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            return maxHandling.computeMax(ds, collection, aggregateNode);

        } else if("min".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            return minHandling.computeMin(ds, collection, aggregateNode);

        } else if("avg".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            return avgHandling.computeAvg(ds, collection, aggregateNode);

        } else if("sum".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            return sumHandling.computeSum(ds, collection, aggregateNode);
        }

        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    public Object computeAggOnFiltered(final String ds, final String collection, final AggregateNode aggregateNode, final List<String> keys) throws OperationException {
        if("count(*)".equalsIgnoreCase(aggregateNode.getAggregateName())
                || "count".equalsIgnoreCase(aggregateNode.getAggregateName())) {

        } else if("max".equalsIgnoreCase(aggregateNode.getAggregateName())) {

        } else if("min".equalsIgnoreCase(aggregateNode.getAggregateName())) {

        } else if("avg".equalsIgnoreCase(aggregateNode.getAggregateName())) {

        } else if("sum".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            return sumHandling.computeSumOnFiltered(ds, collection, aggregateNode, keys);
        }

        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    public Object computeAggOnGroups(final String ds, final String collection, final AggregateNode aggregateNode, Map<String, List<JSONObject>> groupedResult, List<String> groupByColumns) throws OperationException {
        if("count(*)".equalsIgnoreCase(aggregateNode.getAggregateName())
                || "count".equalsIgnoreCase(aggregateNode.getAggregateName())) {

        } else if("max".equalsIgnoreCase(aggregateNode.getAggregateName())) {

        } else if("min".equalsIgnoreCase(aggregateNode.getAggregateName())) {

        } else if("avg".equalsIgnoreCase(aggregateNode.getAggregateName())) {

        } else if("sum".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            return sumHandling.computeSumWithGroups(ds, collection, aggregateNode, groupedResult, groupByColumns);
        }

        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    public Object computeAggOnResult(final String ds, final String collection, final AggregateNode aggregateNode, final Map<String, List<JSONObject>> resultMap) throws OperationException {
        if("count(*)".equalsIgnoreCase(aggregateNode.getAggregateName())
                || "count".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            countHandling.computeCountOnResult(ds, collection, aggregateNode, resultMap);

        } else if("max".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            maxHandling.computeMaxOnResult(ds, collection, aggregateNode, resultMap);

        } else if("min".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            minHandling.computeMinOnResult(ds, collection, aggregateNode, resultMap);

        } else if("avg".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            avgHandling.computeAvgOnResult(ds, collection, aggregateNode, resultMap);

        } else if("sum".equalsIgnoreCase(aggregateNode.getAggregateName())) {
            sumHandling.computeSumOnResult(ds, collection, aggregateNode, resultMap);
        }

        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
}
