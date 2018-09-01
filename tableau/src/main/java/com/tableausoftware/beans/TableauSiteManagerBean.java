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

package com.tableausoftware.beans;

import com.tableausoftware.TableauCredentials;
import com.tableausoftware.TableauException;
import com.tableausoftware.documentation.api.rest.bindings.TableauCredentialsType;
import org.springframework.stereotype.Component;

/**
 * @author sanketsarang
 */
@Component
public class TableauSiteManagerBean implements TableauSiteManager {
    @Override
    public void createSite(String siteId) throws TableauException {
        throw new TableauException();
    }

    @Override
    public void createSite(TableauCredentials tableauCredentials, String siteId) throws TableauException {
        throw new TableauException();
    }
}
