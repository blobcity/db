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
import com.blobcity.db.memory.columns.BTreeColumn;
import com.blobcity.db.memory.columns.Column;
import com.blobcity.db.memory.columns.GeospatialColumn;
import com.blobcity.db.memory.columns.TimeseriesColumn;
import com.blobcity.db.memory.records.JsonRecord;
import com.blobcity.db.schema.IndexTypes;
import com.blobcity.db.util.Performance;
import com.blobcity.lib.data.Record;

import java.io.*;
import java.util.*;

/**
 * Holds records for a single in-memory collection. Same implementation and use for durable and non-durable in-memory
 * collections
 *
 * @author sanketsarang
 */
public class MemCollection implements Serializable {

    private final String ds;
    private final String collection;

    private final String collectionName; //must be fully qualified collection name: "ds.collection"
//    private final Set<Record> insertSet = new HashSet<>(5000000); //5 million, arbitrary value. Should be configurable at collection creation
//    private final THashSet<Record> insertSet = new THashSet<>(180000000);//180 million
    private RecordSet insertSet;
    private final List<RecordSet> sets;
    private Map<String, Column> indexMap = new HashMap<>();

    public MemCollection(final String collectionName) {
        this.collectionName = collectionName;

        String[] parts = collectionName.split(".");
        this.ds = parts[0];
        this.collection = parts[1];

        sets = new ArrayList<>();
        insertSet = new RecordSet(ds, collection);
        sets.add(insertSet);
    }

    public void add(Record record) throws OperationException {
        if(record.uniqueCheckRequired() && sets.parallelStream().anyMatch(set -> set.contains(record))) {
            throw new OperationException(ErrorCode.PRIMARY_KEY_CONFLICT,"A record with the given primary key already exists");
        }

        insertSet.add(record);
    }

    public void add(List<Record> records) throws OperationException {
        if(records.parallelStream().anyMatch(record ->
                record.uniqueCheckRequired() && sets.parallelStream().anyMatch(set -> set.contains(record)))){
            throw new OperationException(ErrorCode.PRIMARY_KEY_CONFLICT,"A record with the given primary key already exists");
        }

        insertSet.addAll(records);

        if(insertSet.size() >= Performance.HASH_BUCKET) {
//            insertSet = new HashSet<>(Performance.HASH_BUCKET * 2);
            insertSet = new RecordSet(this.ds, this.collection);
            sets.add(insertSet);
        }
    }

    public List<Record> getRecords() {
        List<Record> list = Collections.synchronizedList(new ArrayList<>(Performance.HASH_BUCKET * sets.size()));
        sets.parallelStream().forEach(set -> list.addAll(set.getCompleteSet()));
        return list;
    }

    public void remove(Record record) {
        insertSet.remove(record);
        sets.forEach(set -> set.remove(record));
    }

    public void remove(final String _id) {
        Record removeRecord = new JsonRecord("{\"_id\":\"" + _id + "\"");
        insertSet.remove(removeRecord);
        sets.forEach(set -> set.remove(removeRecord));
    }

    /**
     * Update or insert. Updates previous records or inserts a new one
     * @param _id the unique _id of the record to update
     * @param record new record to insert, or update as required
     */
    public void upsert(final String _id, final Record record) throws OperationException {
        remove(_id);
        add(record);
    }

    /**
     * Used to passivate data in the table to disk
     */
    public void passivate() {
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File("")))) {
            oos.writeObject(insertSet);
        }catch (IOException ex){

        }
    }

    /**
     * Used to load data from disk into this table
     */
    public void loadFromDisk() {

    }

    public void createIndex(final String columnName, IndexTypes indexTypes) throws OperationException {
        if(indexMap.containsKey(columnName)) {
            throw new OperationException(ErrorCode.INDEXING_ERROR, "Attempting to index an an inexistent column");
        }

        Column column;

        //TODO: implement this

//        switch(indexTypes) {
//            case BITMAP:
//            case HASHED:
//            case BTREE:
//                indexMap.put(column, new BTreeColumn());
//                break;
//            case TIMESERIES:
//                indexMap.put(column, new TimeseriesColumn());
//                break;
//            case GEO:
//                indexMap.put(column, new GeospatialColumn());
//
//        }
    }

}
