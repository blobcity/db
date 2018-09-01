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

package com.blobcity.db.memory.collection;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.memory.records.JsonRecord;
import com.blobcity.db.util.Performance;
import com.blobcity.lib.data.Record;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * @author sanketsarang
 */
public class RecordSet {

    private final String id;
    private final String ds;
    private final String collection;
    private final Set<Record> recordSet = new HashSet<>(Performance.HASH_BUCKET * 2);
    private boolean requiresPassivation = false;
    private boolean bloomChanged = false;
    private BloomFilter bloomFilter = BloomFilter.create(Funnels.integerFunnel(), Performance.HASH_BUCKET, 0.01);

    public RecordSet(final String ds, final String collection) {
        this.id = UUID.randomUUID().toString();
        this.ds = ds;
        this.collection = collection;
    }

    public void add(final Record record) {
        bloomFilter.put(record.getId());
        recordSet.add(record);
        markChange();
    }

    public void addAll(final Collection<Record> records) {
        records.forEach(record -> this.add(record));
    }

    public void remove(final Record record) {
        if(recordSet.remove(record)) {
            markChange();
        }
    }

    public void remove(final String _id) {
        if(recordSet.remove(new JsonRecord("{\"_id\": \"" + _id + "\"}"))) {
            markChange();
        }
    }

    public boolean contains(Record record) {
        if(bloomFilter.mightContain(record.getId())) {
            return recordSet.contains(record);
        }

        return false;
    }

    public void passivate() throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Operation not supported yet.");
    }

    public void loadFromDisk() throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Operation not supported yet.");
    }

    public boolean hasChanged() {
        return requiresPassivation;
    }

    /**
     * Retrains the bloom filter. Should be called intermittently.
     */
    public void reBloom() {
        if(bloomChanged) {
            bloomFilter = BloomFilter.create(Funnels.integerFunnel(), Performance.HASH_BUCKET, 0.01);
            recordSet.forEach(record -> bloomFilter.put(record.getId()));
        }
    }

    public void markChange() {
        requiresPassivation = true;
        bloomChanged = true;
    }

    public int size() {
        return recordSet.size();
    }

    public Set<Record> getCompleteSet() {
        return this.recordSet;
    }
}
