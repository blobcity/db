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

package com.blobcity.db.transactions;

import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Stores transaction capable operations. This class will at any point in time have only active transaction. The
 * operators on this class must ensure that stale or non-current operations are not retained in the store.
 *
 * @author sanketsarang
 */
@Component
public class TransactionStore {

    private final Map<String, Transactable> map = new HashMap<>();

    public void register(final String txid, Transactable transaction) {
        map.put(txid, transaction);
    }

    public void unregister(final String txid) {
        map.remove(txid);
    }
    
    public Transactable get(final String txid) {
        return map.get(txid);
    }
}
