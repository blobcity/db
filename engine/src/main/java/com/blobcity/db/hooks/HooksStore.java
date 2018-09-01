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

package com.blobcity.db.hooks;

import com.blobcity.hooks.Hook;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author sanketsarang
 */
@Component
public class HooksStore {

    /**
     * Stores hooks as hook-id -> Hook
     */
    private final Map<String, Hook> hookMap = new HashMap<>();

    /**
     * Stores event hooks as ds -> collection -> event hook
     */
    private final Map<String, Map<String, Set<Hook>>> eventHooks = new HashMap<>();

    /**
     * Stores transaction hooks as ds -> collection -> transactionHooks
     */
    private final Map<String, Map<String, Set<Hook>>> transactionHooks = new HashMap<>();

    public void init() {

    }

    public Set<Hook> getEventHooks(final String ds, final String collection) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Set<Hook> getTransactionHooks(final String ds, final String collection) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void registerHook(final String ds, final String collection, final Hook hook) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void unregisterHook(final String hookId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}