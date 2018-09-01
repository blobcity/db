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

package com.blobcity.db.storage;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.memory.collection.MemCollectionStoreBean;
import com.blobcity.lib.data.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Set;

/**
 * @author sanketsarang
 */
@Component
public class BSqlMemoryManager {

    @Autowired
    private MemCollectionStoreBean memCollectionStoreBean;

    public void insert(final String ds, final String collection, final Record record) throws OperationException {
        final String dsAndCollection = ds + "." + collection;
        memCollectionStoreBean.get(dsAndCollection).add(record);
    }

    public Record select(final String ds, final String collection, final String key) throws OperationException {
        final String dsAndCollection = ds + "." + collection;
        //TODO: Implement this

        throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Operation not yet supported");
    }

    public Collection<Record> selectAll(final String ds, final String collection) throws OperationException {
        final String dsAndCollection = ds + "." + collection;
        return memCollectionStoreBean.get(dsAndCollection).getRecords();
    }
 }
