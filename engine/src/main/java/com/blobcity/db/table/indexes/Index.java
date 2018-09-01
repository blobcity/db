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

package com.blobcity.db.table.indexes;

import com.blobcity.db.table.relations.Relation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * IMPORTANT: This file is only used for data storage migration for version 1 to 2.
 *
 * @author sanketsarang
 */
@Deprecated
public class Index implements List, Serializable {

    private List<IndexItem> list = new ArrayList<IndexItem>();

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return list.contains((IndexItem) o);
    }

    /**
     * <p>
     * Checks whether the column with the given names is an index or not. This is an overloaded function</p>
     *
     * @param columnName The name of the column to search for
     * @return Result of the search for the column in the list of indexes
     */
    public boolean contains(String columnName) {
        for (IndexItem indexItem : list) {
            if (indexItem.getColumn().equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator iterator() {
        return list.iterator();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object[] toArray(Object[] ts) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean add(Object e) {
        IndexItem indexItem = (IndexItem) e;
        for (IndexItem item : list) {
            if (item.getColumn().equalsIgnoreCase(indexItem.getColumn())) {
                return false;
            }
        }

        return list.add((IndexItem) e);
    }

    @Override
    public boolean remove(Object o) {
        return list.remove((IndexItem) o);
    }

    public boolean remove(String column) {
        for (IndexItem item : list) {
            if (item.getColumn().equalsIgnoreCase(column)) {
                return remove(item);
            }
        }

        return false;
    }

    @Override
    public boolean containsAll(Collection clctn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean addAll(Collection clctn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean addAll(int i, Collection clctn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean removeAll(Collection clctn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean retainAll(Collection clctn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public Object get(int i) {
        return list.get(i);
    }

    public IndexItem get(String column) {
        for (IndexItem item : list) {
            if (item.getColumn().equalsIgnoreCase(column)) {
                return item;
            }
        }

        return null;
    }

    @Override
    public Object set(int i, Object e) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void add(int i, Object e) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object remove(int i) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int indexOf(Object o) {
        return list.indexOf((IndexItem) o);
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ListIterator listIterator() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ListIterator listIterator(int i) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List subList(int i, int i1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Relation getRelation(int i) {
        return list.get(i).getRelation();
    }

    public Relation getRelation(String column) {
        return this.get(column).getRelation();
    }
}
