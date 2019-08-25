package com.blobcity.db.hygiene;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Column;
import com.blobcity.db.schema.IndexTypes;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.util.FileNameEncoding;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Component
public class IndexCleanupBean {

    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private SchemaStore schemaStore;

    public void cleanUpIndexes(final String ds, final String collection) throws OperationException {
        final Set<String> keys = new HashSet<>(dataManager.selectAllKeys(ds, collection));
        final Schema schema = schemaStore.getSchema(ds, collection);
        final Map<String, Column> columnMap = schema.getColumnMap();

        columnMap.forEach((name, column) -> {
            try {
                switch (column.getIndexType()) {
                    case BTREE:
                        cleanOnDiskBTreeIndex(ds, collection, name, keys);
                }
            }catch(OperationException ex) {
                ex.printStackTrace();
            }
        });
    }

    private void cleanOnDiskBTreeIndex(final String ds, final String collection, final String column, final Set<String> dataKeys) throws OperationException {
        /* Delete unmapped index entry files */
        try (DirectoryStream directoryStream = Files.newDirectoryStream(FileSystems.getDefault().getPath(PathUtil.indexColumnFolder(ds, collection, column)))) {
            Iterator<Path> iterator = directoryStream.iterator();

            final List<String> toDeleteList = new ArrayList<>();
            iterator.forEachRemaining(path -> {
                /* Index value stream */
                try (DirectoryStream columnValueStream = Files.newDirectoryStream(path)) {
                    Iterator<Path> columnValueIterator = columnValueStream.iterator();
                    columnValueIterator.forEachRemaining(keyPath -> {
                        if(!dataKeys.contains(keyPath.getFileName().toString())) {
                            try {
                            toDeleteList.add(PathUtil.indexColumnValueFolder(ds, collection, column, path.getFileName().toString()) + keyPath.getFileName().toString());
                            }catch(OperationException ex) {
                                ex.printStackTrace();
                            }
                        }
                    });
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            });

            toDeleteList.parallelStream().forEach(fileLocation -> {
                try {
                    Files.deleteIfExists(FileSystems.getDefault().getPath(fileLocation));
                }catch(IOException ex) {
                    ex.printStackTrace();
                }
            });
        } catch (IOException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        /* Check if index column value folder is empty. If so delete it */
        try (DirectoryStream directoryStream = Files.newDirectoryStream(FileSystems.getDefault().getPath(PathUtil.indexColumnFolder(ds, collection, column)))) {
            Iterator<Path> iterator = directoryStream.iterator();

            final List<String> toDeleteList = new ArrayList<>();
            iterator.forEachRemaining(path -> {
                try {
                    if (new File(PathUtil.indexColumnValueFolder(ds, collection, column, path.getFileName().toString())).listFiles() == null) {
                        toDeleteList.add(PathUtil.indexColumnValueFolder(ds, collection, column, path.getFileName().toString()));
                    }
                }catch(OperationException ex) {
                    ex.printStackTrace();
                }
            });

            toDeleteList.parallelStream().forEach(fileLocation -> {
                try {
                    new File(fileLocation).delete();
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            });

        } catch (IOException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
}
