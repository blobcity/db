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

import java.util.List;
import java.util.Set;

/**
 *
 * @author sanketsarang
 */
public class TimeseriesColumn<T> implements Column<T> {


    @Override
    public void addEntry(T key, Record valueEntry) {

    }

    @Override
    public void removeEntry(T key, Record valueEntry) {

    }

    @Override
    public Set<Record> selectAll() {
        return null;
    }

    @Override
    public Set<Record> selectEQ(T key) {
        return null;
    }

    @Override
    public Set<Record> selectGTEQ(T value) {
        return null;
    }

    @Override
    public Set<Record> selectGT(T value) {
        return null;
    }

    @Override
    public Set<Record> selectLTEQ(T value) {
        return null;
    }

    @Override
    public Set<Record> selectLT(T value) {
        return null;
    }

    @Override
    public Set<Record> selectNEQ(T value) {
        return null;
    }

    @Override
    public Set<Record> selectIN(List<T> values) {
        return null;
    }

    @Override
    public Set<Record> selectNotIN(List<T> values) {
        return null;
    }

    @Override
    public Set<Record> selectBETWEEN(T fromValue, T toValue) {
        return null;
    }

    @Override
    public Set<Record> selectLIKE(T value) {
        return null;
    }
}
