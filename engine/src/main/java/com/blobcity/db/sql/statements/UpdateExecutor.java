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

package com.blobcity.db.sql.statements;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.storage.BSqlFileManager;
import com.blobcity.db.storage.BSqlMemoryManagerOld;
import com.blobcity.db.constants.BQueryParameters;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.OperatorMapper;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.schema.Schema;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.*;
import com.foundationdb.sql.unparser.NodeToString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Executor for UPDATE statements
 *
 * @authro sanketsarang
 */
@Component
public class UpdateExecutor {

    private static final Logger logger = LoggerFactory.getLogger(UpdateExecutor.class.getName());

    @Autowired
    private BSqlFileManager fileManager;
    @Autowired
    private BSqlMemoryManagerOld memoryManager;
    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private SchemaStore schemaStore;

    private boolean inMemory = false;
    
    public String execute(final String appId, final StatementNode stmt) throws OperationException {
        return execute(appId, (UpdateNode) stmt, false);
    }
    
    public String execute(final String appId, final StatementNode stmt, boolean inMemory) throws OperationException {
        this.inMemory = inMemory;
        return udpate(appId, (UpdateNode) stmt);
    }

    private String udpate(final String appId, UpdateNode node) throws OperationException {
        try {
            SelectNode selectNode = (SelectNode) node.getResultSetNode();
            ResultColumnList resultColumns = selectNode.getResultColumns();

            //Supporting only a single table
            if (selectNode.getFromList().size() > 1) {
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "UPDATE cannot have multiple tables");
            }
            String tableName = selectNode.getFromList().get(0).getTableName().getTableName();

            /* Process the column->value expression */
            Map<String, Object> columnValueMap = extractUpdateMap(resultColumns);

            /* Validate that all columns exist in schema */
            validateColumns(columnValueMap.keySet(), appId, tableName);

            /* Process the filter condition and execute */
            ValueNode whereClause = selectNode.getWhereClause();
            if (whereClause == null) {
                if(!inMemory) {
                    List<String> keys = fileManager.selectAll(appId, tableName);
                    return bulkUpdate(appId, tableName, keys, columnValueMap);
                }
                else {
                    List<String> keys = memoryManager.selectAllKeys(appId, tableName);
                    return bulkUpdate(appId, tableName, keys, columnValueMap);
                }
            } else {
                Set<String> keys = filter(appId, tableName, resultColumns, whereClause);
                return bulkUpdate(appId, tableName, keys, columnValueMap);
            }
        } catch (StandardException ex) {
            logger.error("Invalid SQL. ParseStatement failed: " + node.toString(), ex);
            return new JSONObject().put("ack", "0").put("cause", ex.getMessage()).toString();
        }
    }

    private Set<String> filter(final String appId, final String tableName, final ResultColumnList columns, ValueNode whereClause) throws OperationException, StandardException {
        logger.trace("filter({}, {}, {}, {})", new Object[]{appId, tableName, columns.toString(), new NodeToString().toString(whereClause)});

        Set<String> leftResult;
        Set<String> rightResult;
        ValueNode leftOperand;
        ValueNode rightOperand;
        boolean leftSupported;
        boolean rightSupported;
        String column;
        switch (whereClause.getNodeType()) {
            case NodeTypes.AND_NODE:
                AndNode andNode = (AndNode) whereClause;
                leftResult = filter(appId, tableName, columns, andNode.getLeftOperand());
                rightResult = filter(appId, tableName, columns, andNode.getRightOperand());
                return intersect(leftResult, rightResult);
            case NodeTypes.OR_NODE:
                OrNode orNode = (OrNode) whereClause;
                leftResult = filter(appId, tableName, columns, orNode.getLeftOperand());
                rightResult = filter(appId, tableName, columns, orNode.getRightOperand());
                return union(leftResult, rightResult);
            case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
            case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
            case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
            case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
            case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
            case NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
                BinaryRelationalOperatorNode binaryRelationalOperatorNode = (BinaryRelationalOperatorNode) whereClause;
                //TODO support nested select clauses
                leftOperand = binaryRelationalOperatorNode.getLeftOperand();
                rightOperand = binaryRelationalOperatorNode.getRightOperand();
                leftSupported = leftOperand.getNodeType() == NodeTypes.COLUMN_REFERENCE || (leftOperand instanceof ConstantNode);
                rightSupported = rightOperand.getNodeType() == NodeTypes.COLUMN_REFERENCE || (rightOperand instanceof ConstantNode);
                if (!(leftSupported && rightSupported)) {
                    throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED,
                            "Unsupported binary relation : " + binaryRelationalOperatorNode.toString());
                }
                //TODO call different overloaded method based on operand type
                column = ((ColumnReference) leftOperand).getColumnName();
                Object refValue = null;
                if (rightOperand instanceof NumericConstantNode) {
                    refValue = ((NumericConstantNode) rightOperand).getValue();
                } else if (rightOperand instanceof CharConstantNode) {
                    refValue = ((CharConstantNode) rightOperand).getValue();
                } else if (rightOperand instanceof BooleanConstantNode) {
                    refValue = ((BooleanConstantNode) rightOperand).getBooleanValue();
                } else if (rightOperand instanceof BitConstantNode) {
                    refValue = ((BitConstantNode) rightOperand).getValue();
                }
                int numResultCols = columns.size();
                List<String> resultColList = new ArrayList<>();
                for (int index = 0; index < numResultCols; index++) {
                    /* ResultColumns are 1-based */
                    String colName = columns.get(index).getColumnName();
                    resultColList.add(colName);
                    logger.debug("resultCol: " + colName);
                }
                logger.debug("selectKeysWithPattern({}, {}, {}, {}, {})",
                        new Object[]{appId, tableName, column, refValue, binaryRelationalOperatorNode.getOperator()});
                Iterator<String> keys = dataManager.selectKeysWithPattern(appId, tableName, resultColList, column, refValue, OperatorMapper.map(binaryRelationalOperatorNode.getOperator()));

                return toSet(keys);
            case NodeTypes.IN_LIST_OPERATOR_NODE:
                return processIn(appId, tableName, columns, whereClause);
            default:
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Unsupported clause : " + whereClause.toString());
        }
    }

    private Set<String> intersect(Set<String> resultSet1, Set<String> resultSet2) {
        resultSet1.retainAll(resultSet2);
        return resultSet1;
    }

    private Set<String> union(Set<String> resultSet1, Set<String> resultSet2) {
        resultSet1.addAll(resultSet2);
        return resultSet1;
    }

    private String bulkUpdate(final String appId, final String table, final Collection<String> keys, final Map<String, Object> columnData) throws OperationException {
        JSONObject jsonResponse;
        AtomicLong rowsUpdatedCounter = new AtomicLong(0);

        //TODO: Change to parallel stream. This was made regular stream as the index counting for save operations is not thread safe.
        keys.parallelStream().forEach(key -> {
            dataManager.updateAsync(appId, table, key, columnData);
            rowsUpdatedCounter.incrementAndGet();
//            try {
//                JSONObject jsonObject = dataManager.select(appId, table, key);
//                columnData.forEach((k, v) -> jsonObject.put(k, v));
//                dataManager.save(appId, table, jsonObject);
//                rowsUpdatedCounter.incrementAndGet();
//            } catch (Exception ex) {
//                logger.error(ex.getMessage(), ex);
//            }
        });

        try {
            jsonResponse = new JSONObject();
            jsonResponse.put(BQueryParameters.ACK, "1");
            jsonResponse.put(BQueryParameters.ROWS, rowsUpdatedCounter.get());
        } catch (JSONException ex) {
            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        return jsonResponse.toString();
    }

    private Set<String> toSet(Iterator<String> iterator) {
        Set<String> set = new HashSet<>();
        iterator.forEachRemaining(item -> set.add(item));
        return set;
    }

    private Set<String> processIn(final String appId, final String tableName, final ResultColumnList columns, ValueNode whereClause) throws OperationException {
        Set<String> resultSet = new HashSet<>();
        InListOperatorNode inListOperatorNode = (InListOperatorNode) whereClause;
        ValueNode rightOperand = inListOperatorNode.getRightOperandList();
        ValueNodeList valueNodeList = inListOperatorNode.getRightOperandList().getNodeList();
        String column = inListOperatorNode.getLeftOperand().getColumnName();
        logger.debug("selectKeysWithPattern({}, {}, {}, {})",
                new Object[]{appId, tableName, column, "IN"});
        valueNodeList.iterator().forEachRemaining(valueNode -> {
            Object refValue = ((CharConstantNode) valueNode).getValue();
            try {
                int numResultCols = columns.size();
                List<String> resultColList = new ArrayList<>();
                for (int index = 0; index < numResultCols; index++) {
                    /* ResultColumns are 1-based */
                    String colName = columns.get(index).getColumnName();
                    resultColList.add(colName);
                    logger.debug("resultCol: " + colName);
                }
                Iterator<String> keys = dataManager.selectKeysWithPattern(appId, tableName, resultColList, column, refValue, OperatorMapper.map("IN"));
                resultSet.addAll(toSet(keys));
            } catch (OperationException ex) {
                logger.debug("Failed to load data for value " + refValue + " inside IN clause", ex);
            }
        });

        return resultSet;
    }

    private Map<String, Object> extractUpdateMap(ResultColumnList resultColumnList) {
        Map<String, Object> map = new HashMap<>();

        for (ResultColumn resultColumn : resultColumnList) {
            ValueNode expressionNode = resultColumn.getExpression(); //contains the column value
            String columnName = resultColumn.getReference().getColumnName(); //contains the column name
            Object columnValue = null;
            if (expressionNode instanceof NumericConstantNode) {
                columnValue = ((NumericConstantNode) expressionNode).getValue();
            } else if (expressionNode instanceof CharConstantNode) {
                columnValue = ((CharConstantNode) expressionNode).getValue();
            } else if(expressionNode instanceof BooleanConstantNode) {
                columnValue = ((BooleanConstantNode) expressionNode).getValue();
            }

            if (columnValue != null) {
                map.put(columnName, columnValue);
            }
        }

        return map;
    }

    private void validateColumns(Set<String> columnNames, String appId, String tableName) throws OperationException {
        Schema schema = schemaStore.getSchema(appId, tableName);
        Set<String> existingColumns = schema.getColumnMap().keySet();
        for (String columnName : columnNames) {
            if (!existingColumns.contains(columnName)) {
                throw new OperationException(ErrorCode.UNKNOWN_COLUMN, "Unknown column " + columnName);
            }
        }
    }
}
