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

import com.blobcity.db.code.RestrictedClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import com.blobcity.db.annotations.ProcedureStore;

/**
 *
 * @author sanketsarang
 */
@Ignore
public class ClassLoaderTest {

    private static final String CODEBASE = "test/deploy-db-hot/";
    private RestrictedClassLoader classLoader;

    public ClassLoaderTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        classLoader = new RestrictedClassLoader(CODEBASE);
    }

    @After
    public void tearDown() {
    }

    // TODO: Evaluate the validity of this test
    @Test
    public void testProcedureLoad() {
        System.out.println("Begin: testing procedure class loading");
        try {
            Class clazz = classLoader.loadClass("storedprocedure.MyStoredProcedure", true);
            assertTrue(clazz.isAnnotationPresent(com.blobcity.db.annotations.ProcedureStore.class));
            ProcedureStore procedureStore = (com.blobcity.db.annotations.ProcedureStore) clazz.getAnnotation(com.blobcity.db.annotations.ProcedureStore.class);
            System.out.println("Procedure Store Name: " + procedureStore.name());
        } catch (ClassNotFoundException ex) {
            LoggerFactory.getLogger(ClassLoaderTest.class.getName()).error(null, ex);
            fail("Procedure loading failed");
        }
        System.out.println("End: procedure class loading");
    }
}
