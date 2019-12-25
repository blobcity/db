package com.blobcity.db.code;

import com.blobcity.db.bquery.SQLExecutorBean;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.BSqlDatastoreManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SPConfigBean {

    private static final Logger logger = LoggerFactory.getLogger(SPConfigBean.class);

    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private BSqlDatastoreManager dsManager;
    @Autowired
    private SQLExecutorBean sqlExecutor;
    @Autowired @Lazy
    private CodeLoader codeLoader;

    public void loadAllConfigs() {
        List<JSONObject> configList = null;
        try {
            configList = dataManager.selectAll(".systemdb", "SPJars");
        } catch (OperationException e) {
            logger.error("Loading of stored procedure jars will fail for all datastores", e);
            return;
        }
        configList.forEach(config -> {
            final String ds = config.getString("ds");
            final String jar = config.getString("jar");
            try {
                codeLoader.loadJar(ds, jar, false);
            }catch(OperationException ex) {
                logger.error("Error loading jar for ds=" + ds + " and jar=" + jar);
            }
        });
    }

    public void registerJar(final String ds, final String jar) throws OperationException {
        String responseStr = sqlExecutor.executePrivileged(".systemdb", "select * from `.systemdb`.`SPJars` where `ds` = '" + ds + "' and `jar` = '" + jar +  "'");
        JSONObject responseJson = new JSONObject(responseStr);
        if(!responseJson.has("ack")) throw new OperationException(ErrorCode.CODE_LOAD_ERROR);
        if(!responseJson.getJSONArray("p").isEmpty()) {
            return;
        }
        final JSONObject record = new JSONObject();
        record.put("ds", ds);
        record.put("jar", jar);
        dataManager.insert(".systemdb", "SPJars", record);
    }

    public void unregisterStoredProcedures(final String ds) {
        sqlExecutor.executePrivileged(".systemdb", "delete from `.systemdb`.`SPJars` where `ds` = '" + ds + "'");
    }
}