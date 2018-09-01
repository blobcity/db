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

package com.blobcity.db.indexing;

import com.blobcity.db.schema.IndexTypes;
import static com.blobcity.db.schema.IndexTypes.HASHED;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 *
 * @author sanketsarang
 */
@Component
public class IndexFactory {

    @Autowired
    @Qualifier("OnDiskBTreeIndex")
    private IndexingStrategy bTreeIndexingStrategy;
    @Autowired
    @Qualifier("OnDiskHashedIndex")
    private IndexingStrategy hashedIndexingStrategy;
    @Autowired
    @Qualifier("OnDiskUniqueIndex")
    private IndexingStrategy uniqueIndexingStrategy;

    public IndexingStrategy getStrategy(final IndexTypes indexType) {
        switch (indexType) {
            case BTREE:
                return bTreeIndexingStrategy;
            case HASHED:
                return hashedIndexingStrategy;
            case UNIQUE:
                return uniqueIndexingStrategy;
            case NONE:
                return null;
        }

        return null;
    }

    public IndexingStrategy getStrategyInstance(final IndexTypes indexType) {
        switch (indexType) {
            case BTREE:
                return new OnDiskBTreeIndex();
            case HASHED:
                return new OnDiskHashedIndex();
            case UNIQUE:
                return new OnDiskUniqueIndex();
            case NONE:
                return null;
        }

        return null;
    }
}
