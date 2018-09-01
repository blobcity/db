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

package com.blobcity.db.transientstate;

import org.apache.mina.util.ConcurrentHashSet;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author sanketsarang
 */
@Component //singleton
public class TransientStore {
    public Set<String> dsSet = new ConcurrentHashSet<>();
    public Set<String> collectionSet = new ConcurrentHashSet<>();
    public Set<String> recordSet = new ConcurrentHashSet<>();

    public boolean acquireDsPermit(final String ds) {
        return dsSet.add(ds);
    }

    public void releaseDsPermit(final String ds) {
        dsSet.remove(ds);
    }

    public boolean isDsTransient(final String ds) {
        return dsSet.contains(ds);
    }

    public boolean acquireCollectionPermit(final String ds, final String collection) {
        final String fullCollectionName = ds + "." + collection;
        return collectionSet.add(fullCollectionName);
    }

    public void releaseCollectionPermit(final String ds, final String collection) {
        final String fullCollectionName = ds + "." + collection;
        collectionSet.remove(fullCollectionName);
    }

    public boolean isCollectionTransient(final String ds, final String collection) {
        final String fullCollectionName = ds + "." + collection;
        return collectionSet.contains(fullCollectionName);
    }

    public boolean acquireRecordPermit(final String ds, final String collection, final String pk) {
        final String fullPk = ds + "." + collection + "." + pk;
        return recordSet.add(fullPk);
    }

    public void releaseRecordPermit(final String ds, final String collection, final String pk) {
        final String fullPk = ds + "." + collection + "." + pk;
        recordSet.remove(fullPk);
    }

    public boolean isRecordTransient(final String ds, final String collection, final String pk) {
        final String fullPk = ds + "." + collection + "." + pk;
        return recordSet.contains(fullPk);
    }
}
