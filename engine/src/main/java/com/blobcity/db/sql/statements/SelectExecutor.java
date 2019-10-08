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

import com.blobcity.db.billing.SelectActivityLog;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.cache.QueryResultCache;
import com.blobcity.db.features.FeatureRules;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.sql.processing.OnDiskAggregateHandling;
import com.blobcity.db.sql.processing.OnDiskGroupByHandling;
import com.blobcity.db.sql.processing.OnDiskSumHandling;
import com.blobcity.db.sql.processing.OnDiskWhereHandling;
import com.blobcity.db.storage.BSqlFileManager;
import com.blobcity.db.storage.BSqlMemoryManagerOld;
import com.blobcity.db.constants.BQueryParameters;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.OperatorMapper;
import com.blobcity.db.util.ConsumerUtil;
import com.blobcity.json.JSON;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.*;
import com.foundationdb.sql.unparser.NodeToString;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mina.util.ConcurrentHashSet;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Executor for SELECT statements
 *
 * @author akshaydewan
 * @author sanketsarang
 * @author kritisingh1
 */
@Component
public class SelectExecutor {

    private static final Logger logger = LoggerFactory.getLogger(SelectExecutor.class.getName());

    @Autowired
    @Lazy
    private BSqlCollectionManager tableManager;
    @Autowired
    @Lazy
    private BSqlFileManager fileManager;
    @Autowired
    @Lazy
    private BSqlMemoryManagerOld memoryManager;
    @Autowired
    @Lazy
    private BSqlDataManager dataManager;
    @Autowired
    @Lazy
    private BSqlIndexManager indexManager;
    @Autowired
    @Lazy
    private SchemaManager schemaManager;
    @Autowired
    @Lazy
    private SchemaStore schemaStore;

    /* Aggregate handling functions */
    @Autowired
    @Lazy
    private OnDiskSumHandling onDiskSumHandling;
    @Autowired
    @Lazy
    private OnDiskAggregateHandling onDiskAggregateHandling;
    @Autowired
    @Lazy
    private OnDiskGroupByHandling onDiskGroupByHandling;
    @Autowired
    @Lazy
    private OnDiskWhereHandling onDiskWhereHandling;
    @Autowired
    @Lazy
    private QueryResultCache queryResultCache;
    @Autowired
    private SelectActivityLog selectActivityLog;

    private boolean inMemory = false;

    public String execute(final String appId, final StatementNode stmt, final String sqlString) throws OperationException {
        return execute(appId, stmt, false, sqlString);
    }

    public String execute(final String appId, final StatementNode stmt, boolean inMemory, final String sqlString) throws OperationException {
        this.inMemory = inMemory;
        return select(appId, (CursorNode) stmt, sqlString);
    }

    private String select(final String appId, CursorNode node, final String sqlString) throws OperationException {
        final long startTime = System.currentTimeMillis();
        try {
            SelectNode selectNode = (SelectNode) node.getResultSetNode();
            ResultColumnList resultColumns = selectNode.getResultColumns();

            int limit = -1;
            if(sqlString.contains("limit")) {
                limit = Integer.parseInt(sqlString.substring(sqlString.indexOf("limit") + 6));
            }

            //Supporting only a single table
            if (selectNode.getFromList().size() > 1) {
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "SELECT from multiple tables is not supported at present");
            }
            String tableName = selectNode.getFromList().get(0).getTableName().getTableName();
            if (tableManager.isInMemory(appId, tableName)) {
                inMemory = true;
            }

            /* Load query result from cache if present in cache */
            if(FeatureRules.QUERY_RESULT_CACHING) {
                final String result = queryResultCache.get(sqlString);
                if (result != null) {
                    logger.trace("Returning cached response for SQL query: " + sqlString);
                    return result;
                }
            }

            String schema = selectNode.getFromList().get(0).getTableName().getSchemaName();

            ValueNode whereClause = selectNode.getWhereClause();
            GroupByList groupByList = selectNode.getGroupByList();
            ValueNode havingClause = selectNode.getHavingClause();
            OrderByList orderByList = node.getOrderByList();

            /**
             * General order of execution of SQL statements
             *
             * 1. Run the WHERE clause. Get keys or get records. In-memory could always get records
             * 2. Execute GROUP BY on the where result
             * 3. Execute any aggregates. Execute these centrally as HAVING may also use the aggregate result
             * 4. Filter the result set with the HAVING condition
             * 5. Extract DISTINCT values from the result
             * 6. ORDER the result
             */

            /**
             * Special case handling for performance improvement
             * 1. SELECT DISTINCT Col1 without a WHERE or GROUP BY clause is shorted to Cardinality selection
             * 2. SELECT Col1 without a WHERE or GROUP BY clause is fetched from column index
             * 3. SELECT SUM(Col1), AVG(Col1) is processed using Index instead of Records
             * 4. SELECT MAX(Col1), MIN(Col1) are extracted from max and min from index cardinals
             * 5. SELECT _ from table limit 10
             */

            /* Populate group by columns */
            final List<String> groupByColumns = new ArrayList<>();
            if (groupByList != null) {
                groupByList.forEach(groupByColumn -> {
                    groupByColumns.add(groupByColumn.getColumnName());
                });
            }

            /* Populate aggregate conditions */
            final List<AggregateNode> aggOperations = new ArrayList<>();
            resultColumns.forEach(column -> {
                if (column.getExpression() instanceof AggregateNode) {
                    aggOperations.add((AggregateNode) column.getExpression());
                }
            });

            final Set<String> columnNames = new HashSet<>(); //names of columns selected. Only these columns to be included in result
            final Map<ValueNode, String> columnMap = new HashMap<>(); //ResultColumn object to column mapping
            Iterator<ResultColumn> iterator = resultColumns.iterator();
            resultColumns.forEach(resultColumn -> {
                if (resultColumn.getExpression() instanceof ColumnReference) {
                    columnNames.add((resultColumn.getExpression()).getColumnName());
                    columnMap.put(resultColumn.getExpression(), resultColumn.getExpression().getColumnName());
                } else if (resultColumn.getExpression() instanceof AggregateNode) {
                    columnNames.add((resultColumn.getExpression()).getColumnName());
                    columnMap.put(resultColumn.getExpression(), resultColumn.getExpression().getColumnName());
                }
            });

            final Map<AggregateNode, Object> aggregateMap = new HashMap<>();
            final Map<String, List<JSONObject>> resultMap = new HashMap<>();

            if (schema == null) {
                schema = appId;
            }

            /* Special case handling */
            if (!inMemory) {
                /* OnDisk special case handling */

                /* SELECT COUNT(*) from table */
                if (sqlString.trim().toLowerCase().startsWith("select count(*) from") && whereClause == null && groupByColumns.size() == 0) {
                    return produceCountStarResult((int)onDiskAggregateHandling.computeAgg(appId, tableName, aggOperations.get(0)), startTime).toString();
                }

                /* SELECT DISTINCT Col1 FROM table */
                else if (selectNode.isDistinct() && whereClause == null && aggOperations.size() == 0 && resultColumns.size() == 1
                        && resultColumns.getColumnNames()[0] != null && groupByColumns.size() == 0) {
                    populateSingleColumnDistinct(appId, tableName, resultColumns.getColumnNames()[0], resultMap);
                    if(orderByList != null) {
                        orderResult(appId, tableName, orderByList, resultMap);
                    }
                    return produceResult(appId, tableName, sqlString, resultMap, limit, startTime);
                }

                /* SELECT DISTINCT Col1 FROM table where <conditions> */
                else if (selectNode.isDistinct() && whereClause != null && aggOperations.size() == 0 && resultColumns.size() == 1
                        && resultColumns.getColumnNames()[0] != null && groupByColumns.size() == 0) {
                    populateSingleColumnDistinctWithWhere(appId, tableName, resultColumns.getColumnNames()[0], resultColumns, whereClause, resultMap);
                    if(orderByList != null) {
                        orderResult(appId, tableName, orderByList, resultMap);
                    }
                    return produceResult(appId, tableName, sqlString, resultMap, limit, startTime);
                }

                /* SELECT col1 FROM table */
                if (!selectNode.isDistinct() && whereClause == null && aggOperations.size() == 0 && resultColumns.size() == 1
                        && resultColumns.getColumnNames()[0] != null && groupByColumns.size() == 0) {
                    populateSingleColumn(appId, tableName, resultColumns.getColumnNames()[0], resultMap, limit);
                    if(orderByList != null) {
                        orderResult(appId, tableName, orderByList, resultMap);
                    }
                    return produceResult(appId, tableName, sqlString, resultMap, limit, startTime);
                }

                /* SELECT DISTINCT * FROM table */
                /* SELECT * FROM table */
                else if (whereClause == null && aggOperations.size() == 0 && resultColumns.size() == 1
                        && resultColumns.getColumnNames()[0] == null && groupByColumns.size() == 0) {
                    populateSelectAll(appId, tableName, resultMap, limit);
                    if(orderByList != null) {
                        orderResult(appId, tableName, orderByList, resultMap);
                    }
                    return produceResult(appId, tableName, sqlString, resultMap, limit, startTime);
                }

                /* SELECT SUM(col1) from table */
                /* SELECT SUM(col1),SUM(col2) from table */
                /* SELECT SUM(col1),MIN(col2) from table */
                /* Only aggregates combinations thereof */
                else if (whereClause == null && aggOperations.size() > 0 && resultColumns.size() == aggOperations.size()
                        && groupByColumns.size() == 0) {
                    return produceOnlyAggregateResult(computeFullColumnAggregates(appId, tableName, aggOperations), startTime).toString();
                }

                /* SELECT col2 FROM table GROUP BY col2 */
                /* SELECT col2 FROM table WHERE col1 > 10 GROUP BY col2 */
                /* SELECT SUM(col2) FROM table GROUP BY col2 */
                /* SELECT SUM(col2) FROM table WHERE col1 > 10 GROUP BY col2 */
                //TODO: Implement this


                /* SELECT col1, col2, coln FROM table -> will use only column index */
                /* SELECT DISTINCT col1, col2, coln FROM table -> will use only column index and then perform distinct */
                else if(whereClause == null && aggOperations.isEmpty() && resultColumns.size() > 1 && groupByColumns.size() == 0) {
                    populateOnlyColumnsResult(appId, tableName, columnNames, resultMap);
                    if(selectNode.isDistinct()) {
                        keepDistinct(resultMap);
                    }
                    if(orderByList != null) {
                        orderResult(appId, tableName, orderByList, resultMap);
                    }
                    return produceResult(appId, tableName, sqlString, resultMap, limit, startTime);
                }

            } else {
                /* InMemory special case handling */
            }

            if(!inMemory) {
                Set<String> keys = whereClause == null ? new HashSet<>(dataManager.selectAllKeys(appId, tableName)) : onDiskWhereHandling.executeWhere(appId, tableName, resultColumns, whereClause);
                List<JSONObject> filteredData = loadData(appId, tableName, keys);

                if(groupByList != null) {
                    resultMap.putAll(groupBy(filteredData, groupByList));
                    resultMap.remove(""); //remove empty group
                } else {
                    resultMap.put("_master_", filteredData);
                }

                runAggregates(appId, tableName, aggOperations, resultMap);

                /* Keep only single result per group if results is a grouped by result */
                if(groupByList != null) {
                    if(havingClause != null) {
//                        runHaving()
                    }
                    keepSingleRecordsPerGroup(resultMap);
                } else if(resultColumns.size() == aggOperations.size()) {
                    keepSingleRecordsPerGroup(resultMap);
                }

                if(orderByList != null) {
                    orderResult(appId, tableName, orderByList, resultMap);
                }

                /* Keeps only the requested columns and does not touch if * is present */
                controlColumns(resultColumns, resultMap);

                if(selectNode.isDistinct()) {
                    keepDistinct(resultMap);
                }

                return produceResult(appId, tableName, sqlString, resultMap, limit, startTime);

            } else {
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "In-memory operations temporarily suspended");
            }
        } catch (StandardException ex) {
            logger.error("Invalid SQL. ParseStatement failed: " + node.toString(), ex);
            return new JSONObject().put("ack", "0").put("cause", ex.getMessage()).toString();
        }
    }

    private List<JSONObject> loadData(final String ds, final String collection, final Set<String> keys) throws OperationException {
        final List<JSONObject> list = Collections.synchronizedList(new ArrayList<>());
        keys.parallelStream().forEach(ConsumerUtil.throwsException(_id -> list.add(dataManager.select(ds, collection, _id)), OperationException.class));
        return list;
    }

    private String produceResult(final String ds, final String collection, final String sqlQuery, final Map<String, List<JSONObject>> resultMap, final int limit, final long startTime) throws OperationException {
        final JSONObject responseJson = new JSONObject();
        final List<JSONObject> resultList = new ArrayList<>();
        resultMap.forEach((key, value) -> resultList.addAll(value));

        List<JSONObject> result = resultList;
        if(limit != -1){
            result = result.subList(0, limit);
        }

        final long executionTime = System.currentTimeMillis() - startTime;
        final String resultString = responseJson.put(BQueryParameters.ACK, "1")
                .put(BQueryParameters.PAYLOAD, result)
                .put(BQueryParameters.TIME, executionTime)
                .put(BQueryParameters.ROWS, result.size()).toString();

        if(FeatureRules.QUERY_RESULT_CACHING) {
            queryResultCache.cache(ds, collection, sqlQuery, resultString);
        }

        /* Register the number of rows selected for cloud billing purposes */
        if(!ds.equals(".systemdb")) {
            selectActivityLog.registerActivity(ds, result.size());
        }

        return resultString;
    }

    private void runAggregates(final String ds, final String collection, final List<AggregateNode> aggregateNodes, final Map<String, List<JSONObject>> resultMap) {
        aggregateNodes.forEach(ConsumerUtil.throwsException(aggregateNode -> onDiskAggregateHandling.computeAggOnResult(ds, collection, aggregateNode, resultMap), OperationException.class));
    }

    private void keepSingleRecordsPerGroup(final Map<String, List<JSONObject>> resultMap) {
        resultMap.forEach((key, value) -> {
            if(!value.isEmpty()) {
                JSONObject jsonObject = value.get(0);
                value.clear();
                value.add(jsonObject);
            }
        });
    }

    private void orderResult(final String ds, final String collection, final OrderByList orderByList, final Map<String, List<JSONObject>> resultMap) throws OperationException {
        final List<OrderingColumn> orderingColumnList = new ArrayList<>();
        orderByList.forEach(orderByColumn -> {
            if(orderByColumn.getExpression() instanceof ColumnReference) {
                orderingColumnList.add(new OrderingColumn(((ColumnReference) orderByColumn.getExpression()).getColumnName(), orderByColumn.isAscending()));
            }
        });

        if(orderingColumnList.isEmpty()) {
            return;
        }

        final OrderingColumn orderColumn = orderingColumnList.get(0);

        if(orderingColumnList.size() > 1) {
            logger.warn("ORDER BY on more than one column not supported. Default to ordering on first column only");
        }

        List<JSONObject> data = new ArrayList<>();
        resultMap.forEach((key, value) -> data.addAll(value));

        final FieldType fieldType = schemaStore.getSchema(ds, collection).getColumn(orderColumn.getColumnName()).getFieldType();
        final OrderingComparator orderingComparator = new OrderingComparator(orderColumn, fieldType);

        JSONObject []jsonArr = new JSONObject[data.size()];
        jsonArr = data.toArray(jsonArr);
        Arrays.parallelSort(jsonArr, orderingComparator);

        resultMap.clear();
        resultMap.put("_ordered_", Arrays.asList(jsonArr));
    }

    /**
     * Of the whole result, removes the unwanted columns to keep only those columns requested in the search result
     * @param resultColumns the columns requested in the select query
     * @param resultMap records inside this result map are updated to keep on requested columns
     */
    private void controlColumns(final ResultColumnList resultColumns, final Map<String, List<JSONObject>> resultMap) {
        final Set<String> displayNames = new HashSet<>();

        for (ResultColumn resultColumn : resultColumns) {
            if (resultColumn.getExpression() instanceof ColumnReference) {
                final String columnName = (resultColumn.getExpression()).getColumnName();
                if(columnName != null) {
                    displayNames.add(columnName);
                }
            } else if (resultColumn.getExpression() instanceof AggregateNode) {
                AggregateNode aggNode = (AggregateNode) resultColumn.getExpression();
                displayNames.add(aggNode.getAggregateName() + "(" + ((ColumnReference) aggNode.getOperand()).getColumnName() + ")");
            }
        }

        /* Either only * present or * found in the column list, so ignore filtering */
        if(displayNames.isEmpty() || displayNames.size() != resultColumns.getColumnNames().length) {
            return;
        }

        resultMap.forEach((key, records) -> {
            List<JSONObject> toRemoveList = new ArrayList<>();
            records.parallelStream().forEach(record -> {
                if (record != null) {
                    record.keySet().retainAll(displayNames);

                    if (record.keySet().isEmpty()) {
                        toRemoveList.add(record);
                    }
                }
            });
            records.removeAll(toRemoveList);
        });
    }

    private void keepDistinct(final Map<String, List<JSONObject>> resultMap) {
        final Map<String, List<JSONObject>> newResultMap = new HashMap<>();
        resultMap.forEach((key, list) -> {
            list.forEach(json -> {
                if(newResultMap.values().stream().filter(jsonList -> jsonListContains(jsonList, json)).count() == 0) {
                    if(!newResultMap.containsKey(key)) {
                        newResultMap.put(key, new ArrayList<>());
                    }

                    newResultMap.get(key).add(json);
                }
            });
        });

        resultMap.clear();
        resultMap.putAll(newResultMap);
    }

    private Map<AggregateNode, Object> computeFullColumnAggregates(final String ds, final String collection, final List<AggregateNode> aggOperations) throws OperationException {
        Map<AggregateNode, Object> aggregateMap = new HashMap<>();
        aggOperations.parallelStream().forEach(aggNode -> {
            try {
                aggregateMap.put(aggNode, onDiskAggregateHandling.computeAgg(ds, collection, aggNode));
            } catch (OperationException ex) {
                logger.warn("Error occurred. Putting null value into map", ex);
                aggregateMap.put(aggNode, null);
            }
        });

        if (aggregateMap.containsValue(null)) {
            throw new OperationException(ErrorCode.SELECT_ERROR, "Error occurred in performing aggregate operation. Make sure you are performing aggregate on a numeric column");
        }

        return aggregateMap;
    }

    private void populateSingleColumnDistinct(final String ds, final String collection, final String columnName, final Map<String, List<JSONObject>> resultMap) throws OperationException {
        final Iterator<String> cardinals = indexManager.getCardinals(ds, collection, columnName);
        final FieldType fieldType = schemaStore.getSchema(ds, collection).getColumn(columnName).getFieldType();

        if (resultMap.containsKey("_master_")) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "SELECT query execution encountered an internal error. SelectExecutor.populateSingleColumnDistinct() wrongly invoked");
        }

        final List<JSONObject> jsonList = new ArrayList<>();
        cardinals.forEachRemaining(ConsumerUtil.throwsException(cardinal -> jsonList.add(new JSONObject().put(columnName, fieldType.convert(cardinal))), OperationException.class));

        resultMap.put("_master_", jsonList);
    }

    private void populateSingleColumnDistinctWithWhere(final String ds, final String collection, final String columnName, final ResultColumnList resultColumnList, final ValueNode whereClause, final Map<String, List<JSONObject>> resultMap) throws OperationException {
        try {
            final Set<String> keys = onDiskWhereHandling.executeWhere(ds, collection, resultColumnList, whereClause);
            final Iterator<String> cardinals = indexManager.getCardinals(ds, collection, columnName);
            final Set<String> selectedCardinals = new HashSet<>();

            cardinals.forEachRemaining(ConsumerUtil.throwsException(cardinal -> {
                Iterator<String> indexStream = indexManager.readIndexStream(ds, collection, columnName, cardinal);
                while(indexStream.hasNext()) {
                    if(keys.contains(indexStream.next())) {
                        selectedCardinals.add(cardinal);
                        return;
                    }
                }
            }, OperationException.class));

            final FieldType fieldType = schemaStore.getSchema(ds, collection).getColumn(columnName).getFieldType();

            if (resultMap.containsKey("_master_")) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "SELECT query execution encountered an internal error. SelectExecutor.populateSingleColumnDistinct() wrongly invoked");
            }

            final List<JSONObject> jsonList = new ArrayList<>();
            selectedCardinals.forEach(ConsumerUtil.throwsException(cardinal -> jsonList.add(new JSONObject().put(columnName, fieldType.convert(cardinal))), OperationException.class));
            resultMap.put("_master_", jsonList);
        } catch (StandardException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error in executing WHERE condition");
        }
    }

    private void populateSingleColumn(final String ds, final String collection, final String columnName, final Map<String, List<JSONObject>> resultMap, final int limit) throws OperationException {
        final Iterator<String> cardinals = indexManager.getCardinals(ds, collection, columnName);
        final FieldType fieldType = schemaStore.getSchema(ds, collection).getColumn(columnName).getFieldType();

        if (resultMap.containsKey("_master_")) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "SELECT query execution encountered an internal error. SelectExecutor.populateSingleColumnDistinct() wrongly invoked");
        }

        final List<JSONObject> jsonList = new ArrayList<>();

        if(limit == -1) {
            cardinals.forEachRemaining(ConsumerUtil.throwsException(
                    cardinal -> indexManager.readIndexStream(ds, collection, columnName, cardinal)
                            .forEachRemaining(ConsumerUtil.throwsException(
                                    _id -> jsonList.add(new JSONObject().put(columnName, fieldType.convert(cardinal))), OperationException.class))
                    , OperationException.class));
        } else {
            int count = 0;
            while(count < limit && cardinals.hasNext()) {
                final String cardinal = cardinals.next();
                final Iterator<String> keyIterator = indexManager.readIndexStream(ds, collection, columnName, cardinal);
                while(count++ < limit && keyIterator.hasNext()) {
                    keyIterator.next();
                    jsonList.add(new JSONObject().put(columnName, fieldType.convert(cardinal)));
                }
            }
        }

        resultMap.put("_master_", jsonList);
    }

    private void populateOnlyColumnsResult(final String ds, final String collection, final Set<String> columnNames, final Map<String, List<JSONObject>> resultMap) throws OperationException {
        if (resultMap.containsKey("_master_")) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "SELECT query execution encountered an internal error. SelectExecutor.populateSingleColumnDistinct() wrongly invoked");
        }

        final Map<String, Map<String, String>> columnResultMap = new HashMap<>();
        final Set<String> keysSet = new ConcurrentHashSet<>();
        final Map<String, FieldType> fieldTypeMap = new HashMap<>();
        final List<JSONObject> jsonList = new ArrayList<>();

        columnNames.parallelStream().forEach(ConsumerUtil.throwsException(columnName -> {
            Map<String, String> keyValueMap = new HashMap<>();
            Set<String> internalKeysSet = new HashSet<>();
            fieldTypeMap.put(columnName, schemaStore.getSchema(ds, collection).getColumn(columnName).getFieldType());

            indexManager.getCardinals(ds, collection, columnName).forEachRemaining(ConsumerUtil.throwsException(cardinal -> {
                indexManager.readIndexStream(ds, collection, columnName, cardinal).forEachRemaining(ConsumerUtil.throwsException(_id -> {
                    keyValueMap.put(_id, cardinal);
                    keysSet.add(_id);
                }, OperationException.class));
            }, OperationException.class));
            columnResultMap.put(columnName, keyValueMap);
            keysSet.addAll(internalKeysSet);
        }, OperationException.class));

        keysSet.parallelStream().forEach(_id -> {
            JSONObject jsonObject = new JSONObject();
            columnResultMap.forEach((columnName, map) -> {
                try {
                    if(map.containsKey(_id)) {
                        jsonObject.put(columnName, fieldTypeMap.get(columnName).convert(map.get(_id)));
                    }
                } catch (OperationException e) {
                    e.printStackTrace();
                }
            });
            jsonList.add(jsonObject);
        });

        resultMap.put("_master_", jsonList);
    }

    private Set<String> filter(final String appId, final String tableName, final ResultColumnList columns, ValueNode whereClause) throws OperationException, StandardException {
        logger.debug("filter({}, {}, {}, {})", new Object[]{appId, tableName, columns.toString(), new NodeToString().toString(whereClause)});

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
                } else {
                    // if unknown make it CharConstantNode
                    if (tableManager.isInMemory(appId, tableName)) {
                        logger.debug("rightOperand: " + rightOperand.toString() + ", val: " + rightOperand.getNodeType());
                        refValue = rightOperand.getColumnName();
                    }
                }
                logger.debug("calling selectKeysWithPattern({}, {}, {}, {}, {})",
                        new Object[]{appId, tableName, column, refValue, binaryRelationalOperatorNode.getOperator()});

                List<String> resultColList = new ArrayList<>();
                String[] resultColNames = columns.getColumnNames();
                for (String resultColName : resultColNames) {
                    if (resultColName != null) {
                        logger.debug("resultCol: " + resultColName);
                        resultColList.add(resultColName);
                    }
                }

                Iterator<String> keys = dataManager.selectKeysWithPattern(appId, tableName, resultColList, column, refValue,
                        OperatorMapper.map(binaryRelationalOperatorNode.getOperator()));

                return toSet(keys);
            case NodeTypes.IN_LIST_OPERATOR_NODE:
                return processIn(appId, tableName, columns, whereClause);
            default:
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Unsupported clause : " + whereClause.toString());
        }
    }

    private Set<Object> filterInMemory(final String appId, final String tableName, final ResultColumnList columns, ValueNode whereClause) throws OperationException, StandardException {
        logger.debug("filter({}, {}, {}, {})", new Object[]{appId, tableName, columns.toString(), new NodeToString().toString(whereClause)});

        Set<Object> leftResult;
        Set<Object> rightResult;
        ValueNode leftOperand;
        ValueNode rightOperand;
        boolean leftSupported;
        boolean rightSupported;
        String column;
        switch (whereClause.getNodeType()) {
            case NodeTypes.AND_NODE:
                AndNode andNode = (AndNode) whereClause;
                leftResult = filterInMemory(appId, tableName, columns, andNode.getLeftOperand());
                rightResult = filterInMemory(appId, tableName, columns, andNode.getRightOperand());
                return intersectObject(leftResult, rightResult);
            case NodeTypes.OR_NODE:
                OrNode orNode = (OrNode) whereClause;
                leftResult = filterInMemory(appId, tableName, columns, orNode.getLeftOperand());
                rightResult = filterInMemory(appId, tableName, columns, orNode.getRightOperand());
                return unionObject(leftResult, rightResult);
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
                } else {
                    // if unknown make it CharConstantNode
                    if (tableManager.isInMemory(appId, tableName)) {
                        logger.debug("rightOperand: " + rightOperand.toString() + ", val: " + rightOperand.getNodeType());
                        refValue = rightOperand.getColumnName();
                    }
                }
                logger.debug("calling selectMemoryRecordsWithPattern({}, {}, {}, {}, {})",
                        new Object[]{appId, tableName, column, refValue, binaryRelationalOperatorNode.getOperator()});

                List<String> resultColList = new ArrayList<>();
                String[] resultColNames = columns.getColumnNames();
                for (String resultColName : resultColNames) {
                    if (resultColName != null) {
                        logger.debug("resultCol: " + resultColName);
                        resultColList.add(resultColName);
                    }
                }

                Iterator<Object> records = dataManager.selectMemoryRecordsWithPattern(appId, tableName, resultColList, column, refValue,
                        OperatorMapper.map(binaryRelationalOperatorNode.getOperator()));

                return toSetOfObjects(records);
            case NodeTypes.IN_LIST_OPERATOR_NODE:
                return processInOnMemoryTable(appId, tableName, columns, whereClause);
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

    private Set<Object> intersectObject(Set<Object> resultSet1, Set<Object> resultSet2) {
        resultSet1.retainAll(resultSet2);
        return resultSet1;
    }

    private Set<Object> unionObject(Set<Object> resultSet1, Set<Object> resultSet2) {
        resultSet1.addAll(resultSet2);
        return resultSet1;
    }

    private List<JSONObject> bulkSelect(final String appId, final String table, Collection<String> keys) throws OperationException {
        List<JSONObject> list = Collections.synchronizedList(new ArrayList<>(keys.size()));

        keys.parallelStream().forEach(key -> {
            try {
                list.add(dataManager.select(appId, table, key));
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
            }
        });

        return list;
    }

    private List<JSONObject> bulkSelect(final String appId, final String table, final Set<String> columns, Collection<String> keys) throws OperationException {
        List<JSONObject> list = Collections.synchronizedList(new ArrayList<>(keys.size()));

        keys.parallelStream().forEach(key -> {
            try {
                list.add(dataManager.select(appId, table, key, columns));
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
            }
        });

        return list;
    }

    private Set<String> toSet(Iterator<String> iterator) {
        if (iterator == null) {
            return Collections.EMPTY_SET;
        }
        Set<String> set = new HashSet<>();
        iterator.forEachRemaining(item -> set.add(item));
        return set;
    }

    private Set<Object> toSetOfObjects(Iterator<Object> iterator) {
        if (iterator == null) {
            return Collections.EMPTY_SET;
        }
        Set<Object> set = new HashSet<>();
        iterator.forEachRemaining(item -> set.add(item));
        return set;
    }

    private Set<String> processIn(final String appId, final String tableName, final ResultColumnList columns, ValueNode whereClause) throws OperationException {
        InListOperatorNode inListOperatorNode = (InListOperatorNode) whereClause;
        RowConstructorNode leftOperand = inListOperatorNode.getLeftOperand();
        ValueNodeList valueNodeList = inListOperatorNode.getRightOperandList().getNodeList();
        String column = leftOperand.getNodeList().get(0).getColumnName();//inListOperatorNode.getLeftOperand().getColumnName();
        logger.debug("selectKeysWithPattern({}, {}, {}, {})",
                new Object[]{appId, tableName, column, "IN"});
        Set<Object> inValues = new HashSet<>();
        valueNodeList.iterator().forEachRemaining(valueNode -> inValues.add(((CharConstantNode) valueNode).getValue()));
        int numResultCols = columns.size();
        List<String> resultColList = new ArrayList<>();
        for (int index = 0; index < numResultCols; index++) {
            /* ResultColumns are 1-based */
            String colName = columns.get(index).getColumnName();
            resultColList.add(colName);
            logger.debug("resultCol: " + colName);
        }
        return toSet(dataManager.selectKeysWithPattern(appId, tableName, resultColList, column, inValues, OperatorMapper.map("IN")));
    }

    private Set<Object> processInOnMemoryTable(final String appId, final String tableName, final ResultColumnList columns, ValueNode whereClause) throws OperationException {
        InListOperatorNode inListOperatorNode = (InListOperatorNode) whereClause;
        RowConstructorNode leftOperand = inListOperatorNode.getLeftOperand();
        ValueNodeList valueNodeList = inListOperatorNode.getRightOperandList().getNodeList();
        String column = leftOperand.getNodeList().get(0).getColumnName();//inListOperatorNode.getLeftOperand().getColumnName();
        logger.debug("selectKeysWithPattern({}, {}, {}, {})",
                new Object[]{appId, tableName, column, "IN"});
        Set<Object> inValues = new HashSet<>();
        valueNodeList.iterator().forEachRemaining(valueNode -> inValues.add(((CharConstantNode) valueNode).getValue()));
        int numResultCols = columns.size();
        List<String> resultColList = new ArrayList<>();
        for (int index = 0; index < numResultCols; index++) {
            /* ResultColumns are 1-based */
            String colName = columns.get(index).getColumnName();
            resultColList.add(colName);
            logger.debug("resultCol: " + colName);
        }
        return toSetOfObjects(dataManager.selectMemoryRecordsWithPattern(appId, tableName, resultColList, column, inValues, OperatorMapper.map("IN")));
    }

    private String produceResultForMemoryRecords(final Collection<Object> records, final ResultColumnList columns) throws OperationException {

        //TODO: Temporary code handling select * and select count(*) only
        Iterator<ResultColumn> iterator = columns.iterator();
        while (iterator.hasNext()) {
            ResultColumn resultColumn = iterator.next();
            if (resultColumn.getExpression() instanceof AggregateNode) {
                AggregateNode node = (AggregateNode) resultColumn.getExpression();
                if ("count(*)".equalsIgnoreCase(node.getAggregateName())) {
                    return countResult(records.size());
                }
            }
            //else if (resultColumn.getExpression() instanceof ColumnReference) {
//                ColumnReference columnReference = (ColumnReference) resultColumn.getExpression();
//                columnNameSet.add(columnReference.getColumnName());
//            }
        }

        JSONObject jsonResponse;
        List<String> list = Collections.synchronizedList(new ArrayList<>(records.size()));

        records.parallelStream().forEach(record -> {
            try {
                list.add(record.toString());
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
            }
        });

        try {
            jsonResponse = new JSONObject();
            jsonResponse.put(BQueryParameters.ACK, "1");
            jsonResponse.put(BQueryParameters.PAYLOAD, list);
        } catch (JSONException ex) {
            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
        return jsonResponse.toString();
    }

    private String produceResult(final String appId, final String tableName, final Collection<String> keys, final ResultColumnList columns) throws OperationException {
        final Set<String> columnNameSet = new HashSet<>();

        //TODO: Temporary code handling select * and select count(*) only
        Iterator<ResultColumn> iterator = columns.iterator();
        while (iterator.hasNext()) {
            ResultColumn resultColumn = iterator.next();
            if (resultColumn.getExpression() instanceof AggregateNode) {
                AggregateNode node = (AggregateNode) resultColumn.getExpression();
                if ("count(*)".equalsIgnoreCase(node.getAggregateName())) {
                    return countResult(keys.size());
                }
            } else if (resultColumn.getExpression() instanceof ColumnReference) {
                ColumnReference columnReference = (ColumnReference) resultColumn.getExpression();
                columnNameSet.add(columnReference.getColumnName());
            }
        }

        List<JSONObject> resultList;
        if (!tableManager.isInMemory(appId, tableName)) {
            if (columnNameSet.isEmpty()) {
                resultList = bulkSelect(appId, tableName, keys);
            } else {
                resultList = bulkSelect(appId, tableName, columnNameSet, keys);
            }

            JSONObject responseJson = new JSONObject();
            responseJson.put(BQueryParameters.ACK, "1");
            responseJson.put(BQueryParameters.PAYLOAD, resultList);
            return responseJson.toString();
        }

        return null;
    }

    private String countResult(final long count) {
        JSONObject jsonObject = new JSONObject();
        JSONObject payloadJson = new JSONObject();
        jsonObject.put(BQueryParameters.ACK, "1");
        payloadJson.put(BQueryParameters.COUNT, count);
        jsonObject.put(BQueryParameters.PAYLOAD, payloadJson);
        return jsonObject.toString();
    }

    private String produceSingleColumnSelectDistinctResult(final String column, final Iterator<String> cardinals, FieldType fieldType) {
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();

        cardinals.forEachRemaining(cardinal -> {
            try {
                jsonArray.put(new JSONObject().put(column, fieldType.convert(cardinal)));
            } catch (OperationException e) {
                e.printStackTrace();
            }
        });

        return jsonObject.put(BQueryParameters.ACK, "1").put(BQueryParameters.PAYLOAD, jsonArray).toString();
    }

    private void populateSelectAll(final String ds, final String collection, final Map<String, List<JSONObject>> resultMap, final int limit) throws OperationException {
        if(resultMap.containsKey("_master_")) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "SelectExecutor.populateSelectAll incorrectly invoked");
        }

        if(limit == -1) {
            resultMap.put("_master_", dataManager.selectAll(ds, collection));
        }else {
            resultMap.put("_master_", dataManager.selectAll(ds, collection, limit));
        }
    }

    /**
     * Used to group select or select with where data into the specified groups. GROUP BY is run immediately
     * after the WHERE clause. Data must be selected for the GROUP BY to execute.
     *
     * @param data        the data from SELECT * or SELECT * with WHERE
     * @param groupByList the list of columns to group on. Cannot be null or empty list
     * @return data grouped by the column values
     */
    private Map<String, List<JSONObject>> groupBy(final List<JSONObject> data, GroupByList groupByList) {
        final List<String> groupByColumns = new ArrayList<>();
        groupByList.forEach(groupByColumn -> groupByColumns.add(groupByColumn.getColumnName()));

        final Map<String, List<JSONObject>> groupedMap = new ConcurrentHashMap<>();

        logger.trace("groupBy - started processing individual data records");
        data.parallelStream().forEach(jsonObject -> {
            if(jsonObject != null) {
                StringBuilder sb = new StringBuilder();
                groupByColumns.forEach(columnName -> {
                    if (!jsonObject.has(columnName)) sb.append("-");
                    else sb.append(jsonObject.get(columnName));
                });

                String key = sb.toString();
                if (!groupedMap.containsKey(key)) {
                    groupedMap.put(key, Collections.synchronizedList(new ArrayList<>()));
                }
                groupedMap.get(key).add(jsonObject);
            }
        });

        return groupedMap;
    }

    /**
     * Creates a new JSONObject with ack:1 as the only parameter in it
     *
     * @return new ack:1 as JSONObject
     */
    private JSONObject ack1() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ack", "1");
        return jsonObject;
    }

    /**
     * Used to produce a result where only aggregates are requested
     *
     * @param aggMap
     * @return
     */
    private JSONObject produceOnlyAggregateResult(Map<AggregateNode, Object> aggMap, long startTime) {
        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject = new JSONObject();
        aggMap.forEach((aggNode, value) -> jsonObject.put(aggNode.getAggregateName() + "(" + ((ColumnReference) aggNode.getOperand()).getColumnName() + ")", value));
        jsonArray.put(jsonObject);
        JSONObject responseJson = ack1();
        responseJson.put(BQueryParameters.PAYLOAD, jsonArray);
        responseJson.put(BQueryParameters.TIME, (System.currentTimeMillis() - startTime));
        responseJson.put(BQueryParameters.ROWS, jsonArray.length());
        return responseJson;
    }

    private JSONObject produceCountStarResult(int count, long startTime) {
        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("COUNT(*)", count);
        jsonArray.put(jsonObject);
        JSONObject responseJson = ack1();
        responseJson.put(BQueryParameters.PAYLOAD, jsonArray);
        responseJson.put(BQueryParameters.TIME, (System.currentTimeMillis() - startTime));
        responseJson.put(BQueryParameters.ROWS, jsonArray.length());
        return responseJson;
    }

    private boolean jsonListContains(List<JSONObject> list, final JSONObject jsonObject) {
        final boolean []array = new boolean[1];
        array[0] = false;
        list.forEach(json -> {
            if(!array[0] && JSON.areEqual(json, jsonObject)) {
                array[0] = true;
            }
        });
        return array[0];
    }
}

class OrderingColumn {

    private final String columnName;
    private final boolean ascending;

    public OrderingColumn(final String columnName, final boolean ascending) {
        this.columnName = columnName;
        this.ascending = ascending;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isAscending() {
        return ascending;
    }
}

class OrderingComparator implements Comparator<JSONObject> {

    private final OrderingColumn orderingColumn;
    private final FieldType fieldType;

    public OrderingComparator(final OrderingColumn orderingColumn, final FieldType fieldType) {
        this.orderingColumn = orderingColumn;
        this.fieldType = fieldType;
    }

    public int compare(JSONObject obj1, JSONObject obj2) {
        if(!obj1.has(orderingColumn.getColumnName()) || !obj2.has(orderingColumn.getColumnName())) {
            return 0;
        }

        try {
            validateNumeric(fieldType);
            final Double value1 = Double.valueOf(fieldType.convert(obj1.get(orderingColumn.getColumnName())).toString());
            final Double value2 = Double.valueOf(fieldType.convert(obj2.get(orderingColumn.getColumnName())).toString());

            if(value1 == value2) {
                return 0;
            } else if(orderingColumn.isAscending()) {
                if(value1 > value2) {
                    return 1;
                } else {
                    return -1;
                }
            } else {
                if(value1 > value2) {
                    return -1;
                } else {
                    return 1;
                }
            }
        } catch (OperationException e) {
            e.printStackTrace();
            return 0;
        }

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