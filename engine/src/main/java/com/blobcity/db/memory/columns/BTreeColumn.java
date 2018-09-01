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

package com.blobcity.db.memory.columns;

import com.blobcity.lib.data.Record;

import java.util.*;

/**
 *
 * @author sanketsarang
 */
public class BTreeColumn<T> implements Column<T> {
    private SortedMap<T, Set<Record>> dataMap = new TreeMap<>();

    public void addEntry(T key, Record record) {
        if(!dataMap.containsKey(key)) {
            dataMap.put(key, new HashSet<>());
        }

        dataMap.get(key).add(record);
    }

    public void removeEntry(T key, Record record) {
        if(!dataMap.containsKey(key)) {
            return;
        }

        dataMap.get(key).remove(record);
    }

    public void removeCardinal(T key) {
        dataMap.remove(key);
    }

    public Set<Record> selectAll() {
        Set<Record> returnSet = new HashSet<>();

        dataMap.forEach((K, V) -> returnSet.addAll(V));
        return returnSet;
    }

    public Set<Record> selectEQ(T key) {
        Set<Record> returnSet = dataMap.get(key);
        return returnSet != null ? returnSet : Collections.EMPTY_SET;
    }

    public Set<Record> selectGTEQ(T value) {
        Set<Record> returnSet = new HashSet<>();

        SortedMap<T, Set<Record>> subMap = dataMap.tailMap(value);
        subMap.forEach((K, V) -> returnSet.addAll(V));

        return returnSet;
    }

    public Set<Record> selectGT(T value) {
        Set<Record> returnSet = new HashSet<>();

        SortedMap<T, Set<Record>> subMap = dataMap.tailMap(value);
        subMap.remove(value);
        subMap.forEach((K, V) -> returnSet.addAll(V));

        return returnSet;
    }

    public Set<Record> selectLTEQ(T value) {
        Set<Record> returnSet = new HashSet<>();

        SortedMap<T, Set<Record>> subMap = dataMap.headMap(value);
        subMap.forEach((K, V) -> returnSet.addAll(V));

        return returnSet;
    }

    public Set<Record> selectLT(T value) {
        Set<Record> returnSet = new HashSet<>();

        SortedMap<T, Set<Record>> subMap = dataMap.headMap(value);
        subMap.remove(value);
        subMap.forEach((K, V) -> returnSet.addAll(V));

        return returnSet;
    }

    public Set<Record> selectNEQ(T value) {
        Set<Record> returnSet = new HashSet<>();

        dataMap.forEach((K , V) -> {
            if(K != value) {
                returnSet.addAll(V);
            }
        });

        return returnSet;
    }

    public Set<Record> selectIN(List<T> values) {
        Set<Record> returnSet = new HashSet<>();

        dataMap.forEach((K , V) -> {
            if(values.contains(K)) {
                returnSet.addAll(V);
            }
        });

        return returnSet;
    }

    public Set<Record> selectNotIN(List<T> values) {
        Set<Record> returnSet = new HashSet<>();

        dataMap.forEach((K , V) -> {
            if(!values.contains(K)) {
                returnSet.addAll(V);
            }
        });

        return returnSet;
    }

    public Set<Record> selectBETWEEN(T fromValue, T toValue) {
        Set<Record> returnSet = new HashSet<>();

        SortedMap<T, Set<Record>> subMap = dataMap.subMap(fromValue, toValue);
        subMap.forEach((K, V) -> returnSet.addAll(V));

        return returnSet;
    }

    public Set<Record> selectLIKE(T value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
