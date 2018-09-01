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
public class PrimaryKey implements List, Serializable {

    /* List will store all the columns present in the composite primary key.
     * For non-composite keys, the list will contain only a single value
     */
    private List<String> list = new ArrayList<String>();
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

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
        return list.contains(o.toString());
    }

    @Override
    public Iterator iterator() {
        return list.iterator();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public Object[] toArray(Object[] ts) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public boolean add(Object e) {
        if (list.contains(e.toString())) {
            return false;
        }

        return list.add(e.toString());
    }

    @Override
    public boolean remove(Object o) {
        return list.remove(o.toString());
    }

    @Override
    public boolean containsAll(Collection clctn) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public boolean addAll(Collection clctn) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public boolean addAll(int i, Collection clctn) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public boolean removeAll(Collection clctn) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public boolean retainAll(Collection clctn) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public Object get(int i) {
        return list.get(i);
    }

    @Override
    public Object set(int i, Object e) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public void add(int i, Object e) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public Object remove(int i) {
        return list.remove(i);
    }

    @Override
    public int indexOf(Object o) {
        return list.indexOf(o.toString());
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public ListIterator listIterator() {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public ListIterator listIterator(int i) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

    @Override
    public List subList(int i, int i1) {
        throw new UnsupportedOperationException("Unsupported Operation.");
    }

}
