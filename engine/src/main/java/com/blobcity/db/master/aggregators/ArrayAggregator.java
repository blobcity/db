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

package com.blobcity.db.master.aggregators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author sanketsarang
 */
public class ArrayAggregator<T> implements Aggregator<T> {

    private final List<T> list = new ArrayList<>();

    @Override
    public void add(Collection<T> collection) {
        list.addAll(collection);
    }

    @Override
    public Collection<T> getAggregated() {
        return list;
    }
}
