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
import com.blobcity.db.schema.Column;
import com.blobcity.db.util.ConsumerUtil;
import com.blobcity.db.util.ThrowingConsumer;
import com.foundationdb.sql.parser.*;
import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.sql.lang.Aggregate;
import com.google.common.collect.Iterators;
import org.json.JSONObject;
import org.springframework.stereotype.Component;
import com.blobcity.db.lang.columntypes.FieldType;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.lang.String;
import org.springframework.context.annotation.Lazy;
import org.springframework.beans.factory.annotation.Autowired;
import ucar.nc2.util.HashMapLRU;
import java.util.HashMap;

/**
 * @author sanketsarang
 */
@Component
public class OnDiskAvgHandling {

	@Autowired
    @Lazy
    private BSqlIndexManager indexManager;
    @Autowired
    @Lazy
    private SchemaManager schemaManager;

    public Object computeAvg(final String ds, final String collection, final AggregateNode aggregateNode) throws OperationException {

        ValueNode operand = aggregateNode.getOperand();
        if(operand instanceof ColumnReference) {
            final String columnName = ((ColumnReference) operand).getColumnName();
            Column column = schemaManager.readSchema(ds, collection).getColumn(columnName);
            if(column == null) {
                throw new OperationException(ErrorCode.UNKNOWN_COLUMN, "No column found with name " + columnName);
            }
            FieldType fieldType = column.getFieldType();
            validateNumeric(fieldType);
            Aggregate<Number> average = new Aggregate<>();
            indexManager.getCardinals(ds, collection, columnName).forEachRemaining(ConsumerUtil.throwsException(cardinal -> {
                    final long size = indexManager.getIndexCount(ds, collection, columnName, cardinal);
                    Number value = (Number) fieldType.convert(cardinal);
                    average.addCount(new Double(size));
                    average.add(new Double(value.doubleValue() * size));
            }, OperationException.class));
            average.setAverage();
            return fieldType.convert(average.getAverage());
        } else if (operand instanceof ConstantNode) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "AVG(" + ((ConstantNode) operand).getValue() + ") not supported");
        } else {
	        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
	    }
    }

    public Map<String, Object> computeAvgOnResult(final String ds, final String collection, final AggregateNode aggregateNode, final Map<String, List<JSONObject>> resultMap) throws OperationException {
        final Map<String, Object> aggregateMap = new HashMap<>();

        ValueNode operand = aggregateNode.getOperand();
        if (operand instanceof ColumnReference) {
            final String columnName = ((ColumnReference) operand).getColumnName();
            Column column = schemaManager.readSchema(ds, collection).getColumn(columnName);
            if(column == null) {
                throw new OperationException(ErrorCode.UNKNOWN_COLUMN, "No column found with name " + columnName);
            }
            FieldType fieldType = column.getFieldType();
            validateNumeric(fieldType);
            resultMap.forEach((key, records) -> {
                final Aggregate average = new Aggregate();
                records.parallelStream().forEach(record -> {
                    if(record != null && columnName != null && record.has(columnName) && !record.get(columnName).toString().isEmpty()) {
                        average.add(new Double(record.get(columnName).toString()));
                    }
                });
                average.addCount(records.size());
                average.setAverage();
                try {
                    aggregateMap.put(key, fieldType.convert(average.getAverage()));
                } catch (OperationException e) {
                    e.printStackTrace();
                }
            });

            if(aggregateMap.size() != resultMap.size()) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not produce aggregate result for column "
                        + columnName + " for at least one aggregate group");
            }

            final String aggregateName = aggregateNode.getAggregateName() + "(" + ((ColumnReference) aggregateNode.getOperand()).getColumnName() + ")";
            resultMap.forEach((key, records) -> {
                final Object value = aggregateMap.get(key);
                records.parallelStream().forEach(record -> {
                    if(record != null) {
                        record.put(aggregateName, value);
                    }
                });
            });
        } else if (operand instanceof ConstantNode) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "AVG(" + ((ConstantNode) operand).getValue() + ") not supported");
        }

        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private void validateNumeric(FieldType fieldType) throws OperationException {
        switch (fieldType.getType()) {
            case NUMERIC:
            case DECIMAL:
            case DEC:
            case SMALLINT:
            case INTEGER:
            case INT:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE_PRECISION:
            case LONG:
            case DOUBLE:
                break;
            case LIST_INTEGER:
            case LIST_FLOAT:
            case LIST_LONG:
            case LIST_DOUBLE:
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Aggregate on numeric arrays, currently not supported");
            default:
                throw new OperationException(ErrorCode.SELECT_ERROR, "Attempting to execute aggregate operation on a non-numeric column");
        }
    }
}