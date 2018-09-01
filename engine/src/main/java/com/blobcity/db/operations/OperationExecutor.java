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

package com.blobcity.db.operations;

import com.blobcity.db.exceptions.OperationException;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;

/**
 *
 * @author sanketsarang
 */
@Component
public class OperationExecutor {

    @Autowired
    private OperationFactory operationFactory;

    @Async
    public Future<OperationStatus> startOperation(final String app, final String table, final String opid, OperationTypes operationType, String... params) {
        Operable operable = operationFactory.getOperable(operationType, params);
        try {
            return operable.start(app, table, opid);
        } catch (OperationException ex) {
            LoggerFactory.getLogger(OperationExecutor.class.getName()).error(null, ex);
            return ConcurrentUtils.constantFuture(OperationStatus.ERROR);
        }
    }
}
