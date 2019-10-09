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

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import com.blobcity.db.lang.columntypes.NumberField;
import com.blobcity.db.lang.columntypes.StringField;
import com.blobcity.db.schema.SchemaProperties;
import com.blobcity.db.schema.Types;
import com.blobcity.db.sql.CreateTablePayloadGenerator;
import com.blobcity.db.sql.util.DataTypeMapper;
import com.blobcity.util.json.JsonMessages;
import com.foundationdb.sql.parser.ColumnDefinitionNode;
import com.foundationdb.sql.parser.ConstraintDefinitionNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.TableElementList;
import com.foundationdb.sql.parser.TableElementNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.json.JSONObject;

/**
 * Executor for Create Table statements
 *
 * @author akshaydewan
 */
@Component
public class CreateTableExecutor {

    private final Logger logger = LoggerFactory.getLogger(CreateTableExecutor.class.getName());

    @Autowired
    private BSqlCollectionManager tableManager;

    public String execute(final String appId, final StatementNode stmt) throws OperationException, IOException {
        logger.trace("executing createTable({}, {})", new Object[]{appId, stmt});

        List<String> warnings = new ArrayList<>();
        CreateTableNode node = (CreateTableNode) stmt;
        //Schema name will be ignored
        final String tableName = node.getObjectName().getTableName();
        //TODO existence check
        TableElementList tableElementList = node.getTableElementList();
        CreateTablePayloadGenerator payloadGenerator = new CreateTablePayloadGenerator();

        payloadGenerator.setPrimaryKey(SchemaProperties.PRIMARY_KEY_COL_NAME); //setting pk to forced value
        payloadGenerator.putColumn(SchemaProperties.PRIMARY_KEY_COL_NAME, new StringField(Types.STRING));

        for (TableElementNode tableElementNode : tableElementList) {
            int nodeType = tableElementNode.getNodeType();
            switch (nodeType) {
                case NodeTypes.COLUMN_DEFINITION_NODE:
                    ColumnDefinitionNode colDefNode = (ColumnDefinitionNode) tableElementNode;
                    String colName = colDefNode.getColumnName();
                    CollationChecker.check(colDefNode.getType(), warnings);
                    FieldType fieldType = FieldTypeFactory.fromTypeDescriptor(colDefNode.getType());
                    payloadGenerator.putColumn(colName, fieldType);
                    break;
                case NodeTypes.CONSTRAINT_DEFINITION_NODE:
                    //TODO auto-increment check
                    ConstraintDefinitionNode constraintDefNode = (ConstraintDefinitionNode) tableElementNode;
                    ConstraintDefinitionNode.ConstraintType constraintType = constraintDefNode.getConstraintType();
                    switch (constraintType) {
                        case PRIMARY_KEY: //fall through into next case
//                            ResultColumnList colList = constraintDefNode.getColumnList();
//                            if (colList.size() != 1) {
//                                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Primary Key constraint "
//                                        + "must specify only one column. Multiple columns are not supported");
//                            }
//                            payloadGenerator.setPrimaryKey(colList.get(0).getName());
//                            break;
                        case UNIQUE:
                            ResultColumnList colList = constraintDefNode.getColumnList();
                            if (colList.size() != 1) {
                                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Unique constraint "
                                        + "must specify only one column. Multiple columns are not supported");
                            }
                            payloadGenerator.addUniqueConstraint(colList.get(0).getName());
                            break;
                        default:
                            logger.warn("The constraint {} is not supported. Ignoring this during table creation", constraintType.toString());
                    }
                    break;
                default:
                    logger.warn("Unhandled node type {}. This will be ignored.", tableElementNode.toString());
            }
        }
        JSONObject payload = payloadGenerator.generate();
        logger.info("Generated payload: \n {}", payload);
        tableManager.createTable(appId, tableName, payload);
        if (warnings.isEmpty()) {
            return JsonMessages.SUCCESS_ACKNOWLEDGEMENT;
        } else {
            return JsonMessages.successWithWarnings(warnings).toString();
        }
    }

    //-----setters for mock services -----------------------------------------------------------------------------------
    public void setTableManager(BSqlCollectionManager tableManager) {
        this.tableManager = tableManager;
    }

}
