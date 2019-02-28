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

import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import com.blobcity.db.operations.OperationLogLevel;
import com.blobcity.db.schema.AutoDefineTypes;
import com.blobcity.db.schema.IndexTypes;
import com.blobcity.db.schema.Types;
import com.blobcity.db.sql.util.DataTypeMapper;
import com.blobcity.util.json.JsonMessages;
import com.foundationdb.sql.parser.AlterTableNode;
import com.foundationdb.sql.parser.ColumnDefinitionNode;
import com.foundationdb.sql.parser.ConstraintDefinitionNode;
import com.foundationdb.sql.parser.ModifyColumnNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.TableElementList;
import com.foundationdb.sql.parser.TableElementNode;
import com.foundationdb.sql.parser.StatementType;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author akshaydewan
 */
@Component
public class AlterTableExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AlterTableExecutor.class.getName());

    @Autowired
    private BSqlCollectionManager tableManager;
    @Autowired
    private BSqlIndexManager indexManager;

    public String execute(final String appId, final StatementNode stmt) throws OperationException {
        logger.trace("executing alterTable({}, {})", new Object[]{appId, stmt});
        final List<String> warnings = new ArrayList<>();
        AlterTableNode node = (AlterTableNode) stmt;
        //Schema name will be ignored
        final String tableName = node.getObjectName().getTableName();
        TableElementList tableElementList = node.getTableElementList();
        if (tableElementList.size() >= 2) {
            addColumn(appId, tableName, tableElementList, warnings);
        } else if (tableElementList.size() == 1) {
            TableElementNode tableElementNode = tableElementList.get(0);
            switch (tableElementNode.getNodeType()) {
                case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE:
                    modifyConstraint(appId, tableName, (ModifyColumnNode) tableElementNode);
                    break;
                case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE:
                    modifyNotNullConstraint(appId, tableName, (ModifyColumnNode) tableElementNode);
                    break;
                case NodeTypes.MODIFY_COLUMN_DEFAULT_NODE:
                    modifyColumnDefault(appId, tableName, (ModifyColumnNode) tableElementNode);
                    break;
                case NodeTypes.COLUMN_DEFINITION_NODE:
                    addColumn(appId, tableName, (ColumnDefinitionNode) tableElementNode, warnings);
                    break;
                case NodeTypes.DROP_COLUMN_NODE:
                    dropColumn(appId, tableName, (ModifyColumnNode) tableElementNode, node);
                    break;
                case NodeTypes.CONSTRAINT_DEFINITION_NODE:
                    addConstraint(appId, tableName, (ConstraintDefinitionNode) tableElementNode);
                    break;
                default:
                    logger.error("unsupported node type");
                    throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Statement not supported");
            }
        } else {
            logger.error("unsupported tableElementList");
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Statement not supported");
        }
        if (warnings.isEmpty()) {
            return JsonMessages.SUCCESS_ACKNOWLEDGEMENT;
        } else {
            return JsonMessages.successWithWarnings(warnings).toString();
        }
    }

    private void modifyConstraint(final String appId, final String tableName, final ModifyColumnNode node) throws OperationException {
        logger.trace("modifyConstraint(): {}", node.toString());
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Modifying the specified constratint is not supported");
    }

    private void modifyNotNullConstraint(final String appId, final String tableName, final ModifyColumnNode node) throws OperationException {
        logger.trace("modifyNotNullConstraint(): {}", node.toString());
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "NOT NULL constraint is not supported");
    }

    private void modifyColumnDefault(final String appId, final String tableName, final ModifyColumnNode node) throws OperationException {
        logger.trace("modifyColumnDefault(): {}", node.toString());
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Column defaults are not supported");
    }

    private void addColumn(final String appId, final String tableName, final ColumnDefinitionNode node, final List<String> warnings) throws OperationException {
        /**
         * Adds a column without indexing
         */
        logger.trace("addColumn(): {}", node.toString());
        final String columnName = node.getName();
        CollationChecker.check(node.getType(), warnings);
        FieldType fieldType = FieldTypeFactory.fromTypeDescriptor(node.getType());
        //TODO node.isAutoincrementColumn();
        tableManager.addColumn(appId, tableName, columnName, fieldType, AutoDefineTypes.NONE, IndexTypes.NONE);
    }

    private void addColumn(final String appId, final String tableName, final TableElementList tableElementList, final List<String> warnings) throws OperationException {
        logger.trace("addColumn(): {}", tableElementList.toString());
        String columnName = null;
        FieldType fieldType = null;
        IndexTypes indexType = IndexTypes.NONE;
        for (TableElementNode tableElementNode : tableElementList) {
            int nodeType = tableElementNode.getNodeType();
            switch (nodeType) {
                case NodeTypes.COLUMN_DEFINITION_NODE:
                    ColumnDefinitionNode colDefNode = (ColumnDefinitionNode) tableElementNode;
                    columnName = colDefNode.getColumnName();
                    fieldType = FieldTypeFactory.fromTypeDescriptor(colDefNode.getType());
                    CollationChecker.check(colDefNode.getType(), warnings);
                    break;
                case NodeTypes.CONSTRAINT_DEFINITION_NODE:
                    //TODO auto-increment check
                    ConstraintDefinitionNode constraintDefNode = (ConstraintDefinitionNode) tableElementNode;
                    ConstraintDefinitionNode.ConstraintType constraintType = constraintDefNode.getConstraintType();
                    switch (constraintType) {
                        case PRIMARY_KEY:
                            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Altering Primary Key column is not supported");
                        case UNIQUE:
                            ResultColumnList colList = constraintDefNode.getColumnList();
                            if (colList.size() != 1) {
                                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Unique constraint "
                                        + "must specify only one column. Multiple columns are not supported");
                            }
                            indexType = IndexTypes.UNIQUE;
                        default:
                            logger.warn("The constraint {} is not supported. Ignoring this during table creation", constraintType.toString());
                    }
                    break;
                default:
                    logger.warn("Unhandled node type {}. This will be ignored.", tableElementNode.toString());
            }
            if (columnName == null || fieldType == null) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT);
            }

            tableManager.addColumn(appId, tableName, columnName, fieldType, AutoDefineTypes.NONE, indexType);
        }
    }

    private void dropColumn(final String appId, final String tableName, final ModifyColumnNode node, final AlterTableNode alterTableNode) throws OperationException {
        logger.trace("dropColumn(): {}", node.toString());
        int behavior = alterTableNode.getBehavior();
        if (behavior == StatementType.DROP_CASCADE || behavior == StatementType.DROP_RESTRICT) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "CASCADE/RESTRICT is not supported on DROP COLUMN");
        }
        tableManager.dropColumn(appId, tableName, node.getName());
    }

    private void addConstraint(final String appId, final String tableName, final ConstraintDefinitionNode node) throws OperationException {
        logger.trace("addConstraint(): {}", node.toString());
        ConstraintDefinitionNode.ConstraintType constraintType = node.getConstraintType();
        switch (constraintType) {
            case CHECK:
            case DROP:
            case FOREIGN_KEY:
            case NOT_NULL:
//            case INDEX:
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Constraint " + constraintType.name() + " is not supported");
            //TODO
            //INDEX is not part of standard. 
            case PRIMARY_KEY:
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Primary key cannot be dropped or changed");
            case UNIQUE:
                if (node.getColumnList().size() > 1) {
                    throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Combined UNIQUE indexes are not supported");
                }
                if (node.getColumnList().size() == 0) {
                    throw new OperationException(ErrorCode.INVALID_QUERY);
                }
                String colName = node.getColumnList().get(0).getName();
                indexManager.index(appId, tableName, colName, IndexTypes.UNIQUE, OperationLogLevel.ERROR);
        }
    }

    public void setTableManager(final BSqlCollectionManager tableManager) {
        this.tableManager = tableManager;
    }

    public void setIndexManager(BSqlIndexManager indexManager) {
        this.indexManager = indexManager;
    }

}
