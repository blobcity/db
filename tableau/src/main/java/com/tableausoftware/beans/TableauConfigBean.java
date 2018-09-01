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
import org.springframework.stereotype.Component;

/**
 * @author sanketsarang
 */
@Component
public class TableauConfigBean implements TableauConfig {

    private static TableauConfig instance = null;

    private String configFile;
    private String schemaFile;
    private TableauCredentials tableauCredentials;

    @Override
    public void init(final String configFile, final String schemaFile, final TableauCredentials tableauCredentials) {
        System.out.println("Init tableau");

        this.configFile = configFile;
        this.schemaFile = schemaFile;
        this.tableauCredentials = tableauCredentials;

        instance = this;
    }

    /**
     * Returns an instance of the singleton bean for use by non-spring classes. init must be called before getInstance
     * is invokved.
     * @return returns an instance of {@link TableauConfig} if init is called; {@code null} otherwise
     */
    public static TableauConfig getInstance() {
        return instance;
    }

    public String getConfigFile() {
        return this.configFile;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public TableauCredentials getTableauCredentials() {
        return tableauCredentials;
    }
}
