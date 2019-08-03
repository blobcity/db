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

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.OperatorMapper;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.*;
import com.foundationdb.sql.unparser.NodeToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author sanketsarang
 */
@Component
public class OnDiskWhereHandling {

    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private BSqlCollectionManager tableManager;

    private static final Logger logger = LoggerFactory.getLogger(OnDiskWhereHandling.class.getName());

    /**
     * Executes the complete where clause and returns the keys qualified keys
     * @param ds the datastore
     * @param collection the collection
     * @param columns {@link ResultColumnList} for the specific columns expected in the select result. Could be <code>select *</code>
     * @param whereClause the complete WHERE clause to be executed
     * @return A set of primary keys that qualify the requested WHERE clause
     * @throws OperationException
     * @throws StandardException
     */
    public Set<String> executeWhere(final String ds, final String collection, final ResultColumnList columns, ValueNode whereClause) throws OperationException, StandardException {
        logger.debug("executeWhere({}, {}, {}, {})", new Object[]{ds, collection, columns.toString(), new NodeToString().toString(whereClause)});

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
                leftResult = executeWhere(ds, collection, columns, andNode.getLeftOperand());
                rightResult = executeWhere(ds, collection, columns, andNode.getRightOperand());
                return intersect(leftResult, rightResult);
            case NodeTypes.OR_NODE:
                OrNode orNode = (OrNode) whereClause;
                leftResult = executeWhere(ds, collection, columns, orNode.getLeftOperand());
                rightResult = executeWhere(ds, collection, columns, orNode.getRightOperand());
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
                    if (tableManager.isInMemory(ds, collection)) {
                        logger.debug("rightOperand: " + rightOperand.toString() + ", val: " + rightOperand.getNodeType());
                        refValue = rightOperand.getColumnName();
                    }
                }
                logger.debug("calling selectKeysWithPattern({}, {}, {}, {}, {})",
                        new Object[]{ds, collection, column, refValue, binaryRelationalOperatorNode.getOperator()});

                List<String> resultColList = new ArrayList<>();
                String[] resultColNames = columns.getColumnNames();
                for (String resultColName : resultColNames) {
                    if (resultColName != null) {
                        logger.debug("resultCol: " + resultColName);
                        resultColList.add(resultColName);
                    }
                }

                Iterator<String> keys = dataManager.selectKeysWithPattern(ds, collection, resultColList, column, refValue,
                        OperatorMapper.map(binaryRelationalOperatorNode.getOperator()));

                return toSet(keys);
            case NodeTypes.IN_LIST_OPERATOR_NODE:
                return processIn(ds, collection, columns, whereClause);
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

    private Set<String> toSet(Iterator<String> iterator) {
        if (iterator == null) {
            return Collections.EMPTY_SET;
        }
        Set<String> set = new HashSet<>();
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
}
