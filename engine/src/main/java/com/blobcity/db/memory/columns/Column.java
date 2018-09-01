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
 * Generic representation for all in-memory column stores
 *
 * @author sanketsarang
 */
public interface Column<T> {

    public void addEntry(T key, Record valueEntry);

    public void removeEntry(T key, Record valueEntry);

    public Set<Record> selectAll();

    public Set<Record> selectEQ(T key);

    public Set<Record> selectGTEQ(T value);

    public Set<Record> selectGT(T value);

    public Set<Record> selectLTEQ(T value);

    public Set<Record> selectLT(T value);

    public Set<Record> selectNEQ(T value);

    public Set<Record> selectIN(List<T> values);

    public Set<Record> selectNotIN(List<T> values);

    public Set<Record> selectBETWEEN(T fromValue, T toValue);

    public Set<Record> selectLIKE(T value);
}
