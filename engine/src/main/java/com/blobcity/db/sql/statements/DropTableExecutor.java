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
import com.blobcity.util.json.JsonMessages;
import com.foundationdb.sql.parser.DropTableNode;
import com.foundationdb.sql.parser.ExistenceCheck;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.StatementType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author akshaydewan
 */
@Component
public class DropTableExecutor {

    @Autowired
    private BSqlCollectionManager tableManager;

    public String execute(final String appId, final StatementNode stmt) throws OperationException {
        final DropTableNode node = (DropTableNode) stmt;
        final String tableName = node.getObjectName().getTableName();
        if (node.getExistenceCheck().equals(ExistenceCheck.IF_EXISTS) && !tableExists(appId, tableName)) {
            return JsonMessages.SUCCESS_ACKNOWLEDGEMENT;
        }
        //cascade/restrict not supported
        if (node.getDropBehavior() == StatementType.DROP_CASCADE || node.getDropBehavior() == StatementType.DROP_RESTRICT) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, 
                    "Cascade/Restrict is not supported on DROP TABLE");
        }
        tableManager.dropTable(appId, tableName);
        return JsonMessages.SUCCESS_ACKNOWLEDGEMENT;
    }

    private boolean tableExists(final String appId, final String tableName) throws OperationException {
        return tableManager.exists(appId, tableName);
    }

    public void setTableManager(BSqlCollectionManager tableManager) {
        this.tableManager = tableManager;
    }

}
