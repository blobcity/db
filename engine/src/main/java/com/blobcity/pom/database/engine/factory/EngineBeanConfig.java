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

package com.blobcity.pom.database.engine.factory;

import com.blobcity.code.ExportServiceRouter;
import com.blobcity.code.WebServiceExecutor;
import com.blobcity.db.billing.BillingCron;
import com.blobcity.db.billing.SelectActivityLog;
import com.blobcity.db.bquery.*;
import com.blobcity.db.bsql.*;
import com.blobcity.db.cache.QueryResultCache;
import com.blobcity.db.code.webservices.WebServiceExecutorBean;
import com.blobcity.db.export.ExportProcedureStore;
import com.blobcity.db.export.ExportServiceRouterBean;
import com.blobcity.db.ftp.FtpServerManager;
import com.blobcity.db.ftp.FtpServiceManager;
import com.blobcity.db.hooks.HookableEventBean;
import com.blobcity.db.hooks.HookableTransactionBean;
import com.blobcity.db.indexcache.OnDiskBtreeIndexCache;
import com.blobcity.db.indexing.*;
import com.blobcity.db.mapreduce.MapReduceOutputImporter;
import com.blobcity.db.bquery.statements.BQueryInsertExecutor;
import com.blobcity.db.bquery.statements.BQueryRemoveExecutor;
import com.blobcity.db.bquery.statements.BQuerySelectExecutor;
import com.blobcity.db.bquery.statements.BQueryUpdateExecutor;
import com.blobcity.db.memory.collection.MemCollectionStoreBean;
import com.blobcity.db.opennlp.NLP;
import com.blobcity.db.opennlp.NLPModelsStore;
import com.blobcity.db.processors.ProcessorStore;
import com.blobcity.db.requests.RequestHandlingBean;
import com.blobcity.db.security.ApiKeyManager;
import com.blobcity.db.spamdetect.SpamDetector;
import com.blobcity.db.sql.processing.*;
import com.blobcity.db.sql.statements.*;
import com.blobcity.db.storage.BSqlFileManager;
import com.blobcity.db.storage.BSqlMemoryManager;
import com.blobcity.db.storage.BSqlMemoryManagerOld;
import com.blobcity.db.cache.CacheRules;
import com.blobcity.db.cache.DataCache;
import com.blobcity.db.cli.statements.DDLStatement;
import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.cluster.connection.ConnectionManager;
import com.blobcity.db.cluster.connection.ConnectionStore;
import com.blobcity.db.master.MasterStore;
import com.blobcity.db.cluster.messaging.ClusterMessaging;
import com.blobcity.db.cluster.nodes.NodeDiscovery;
import com.blobcity.db.cluster.nodes.NodeManager;
import com.blobcity.db.cluster.nodes.ProximityNodesStore;
import com.blobcity.db.tableau.TableauPublishManager;
import com.blobcity.db.tableau.TableauPublishStore;
import com.blobcity.db.tableau.TableauTdeManager;
import com.blobcity.db.transaction.CentralCommitLogWriter;
import com.blobcity.db.transactions.TransactionStore;
import com.blobcity.db.code.CodeExecutor;
import com.blobcity.db.code.CodeLoader;
import com.blobcity.db.code.filters.FilterParallelExecutor;
import com.blobcity.db.code.LoaderStore;
import com.blobcity.db.code.ManifestParserBean;
import com.blobcity.db.code.datainterpreter.InterpreterExecutorBean;
import com.blobcity.db.code.datainterpreter.InterpreterStoreBean;
import com.blobcity.db.code.filters.ThreadRun;
import com.blobcity.db.code.filters.FilterExecutorBean;
import com.blobcity.db.code.filters.FilterStoreBean;
import com.blobcity.db.code.procedures.ProcedureExecutorBean;
import com.blobcity.db.code.procedures.ProcedureStoreBean;
import com.blobcity.db.code.triggers.TriggerExecutorBean;
import com.blobcity.db.code.triggers.TriggerStoreBean;
import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.data.RowCountManager;
import com.blobcity.db.data.RowCountStore;
import com.blobcity.db.export.CsvExporter;
import com.blobcity.db.global.live.GlobalLiveManager;
import com.blobcity.db.global.live.GlobalLiveStore;
import com.blobcity.db.home.HomeReportingBean;
import com.blobcity.db.importer.CsvImporter;
import com.blobcity.db.indexing.OnDiskBTreeIndex;
import com.blobcity.db.lang.datatypes.converters.DoubleConverter;
import com.blobcity.db.lang.datatypes.converters.FloatConverter;
import com.blobcity.db.lang.datatypes.converters.IntConverter;
import com.blobcity.db.lang.datatypes.converters.LongConverter;
import com.blobcity.db.lang.datatypes.converters.StringConverter;
import com.blobcity.db.lang.datatypes.converters.TypeConverterFactory;
import com.blobcity.db.locks.MasterLockBean;
import com.blobcity.db.locks.RecordLockBean;
import com.blobcity.db.locks.TransactionLocking;
import com.blobcity.db.mapreduce.JarManager;
import com.blobcity.db.mapreduce.MapReduceDriver;
import com.blobcity.db.mapreduce.MapReduceExecutor;
import com.blobcity.db.mapreduce.MapReduceJobManager;
import com.blobcity.db.memory.old.MemorySearch;
import com.blobcity.db.memory.old.MemoryTableStore;
import com.blobcity.db.olap.DataCubeManager;
import com.blobcity.db.olap.DataCubeStore;
import com.blobcity.db.operations.ActiveOperationStore;
import com.blobcity.db.operations.OperationExecutor;
import com.blobcity.db.operations.OperationFactory;
import com.blobcity.db.operations.OperationLogger;
import com.blobcity.db.operations.OperationQueue;
import com.blobcity.db.operations.OperationsFileStore;
import com.blobcity.db.operations.OperationsManager;
import com.blobcity.db.requests.RequestStoreBean;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.startup.ClusterStartup;
import com.blobcity.db.startup.StartupHandler;
import com.blobcity.db.startup.StorageStartup;
import com.blobcity.db.systemdb.SystemDBService;
import com.blobcity.db.security.UserManager;
import com.blobcity.db.security.UserGroupManager;
import com.blobcity.db.transaction.CollectionCommitLogWriter;
import com.blobcity.db.transientstate.TransientStore;
import com.blobcity.db.versioning.Version1to2;
import com.blobcity.db.versioning.Version2to3;
import com.blobcity.db.versioning.Version3to4;
import com.blobcity.db.versioning.VersionUpgradeFactory;
import com.blobcity.db.watchservice.*;
import com.blobcity.db.code.webservices.WebServiceStore;
import com.blobcity.hooks.HookableEvent;
import com.blobcity.hooks.HookableTransaction;
import com.blobcity.lib.database.bean.manager.factory.ModuleApplicationContextHolder;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryStore;
import com.blobcity.pom.database.engine.bean.ApplicationContextHolder;
import com.blobcity.query.QueryExecutorImpl;
import com.tableausoftware.beans.TableauConfig;
import com.tableausoftware.beans.TableauSiteManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.EnableScheduling;
import com.tableausoftware.beans.TableauSiteManagerBean;
import com.tableausoftware.beans.TableauConfigBean;

/**
 * Configures the creation of beans required by the application along with their scopes. This is the Java equivalent of
 * Spring beans yesteryear.
 *
 * Reference Section 4.12 (Java-based container configuration) in Spring Framework v4.0.3 Documentation
 *
 * @see http://docs.spring.io/spring/docs/4.0.3.RELEASE/spring-framework-reference/htmlsingle/#beans-java
 *
 * @author javatarz (Karun Japhet)
 */
@EnableScheduling
@Configuration
public class EngineBeanConfig {

    private static final Logger logger = LoggerFactory.getLogger(EngineBeanConfig.class);

    /*
     * Startup beans: Start
     */
    @Bean
    public NodeDiscovery nodeDiscovery() { // startup bean
        logger.trace("Creating an instance of NodeDiscovery");

        return new NodeDiscovery();
    }

    @Bean
    public ConfigBean configBean() { // startup bean
        logger.trace("Creating an instance of ConfigBean");

        return new ConfigBean();
    }

    @Bean
    public StartupHandler startupHandler() { // startup bean
        logger.trace("Creating an instance of StartupHandler");

        return new StartupHandler();
    }

    @Bean
    public StorageStartup storageStartup() { // startup bean
        logger.trace("Creating an instance of StorageStartup");

        return new StorageStartup();
    }

    @Bean
    public ClusterStartup clusterStartup() {
        logger.trace("Creating an instance of ClusterStartup");

        return new ClusterStartup();
    }

    @Bean
    public TableauPublishStore tableauPublishStore() {
        logger.trace("Creating an instance of TableauPublishStore");

        return new TableauPublishStore();
    }

    @Bean
    public NLPModelsStore nlpModelsStore() {
        logger.trace("Creating an instance of NLPModelsStore");

        return new NLPModelsStore();
    }

    @Bean(name = "SpamDetector")
    public SpamDetector spamDetector() {
        logger.trace("Creating an instance of SpamDetector");

        return new SpamDetector();
    }

    @Bean(name = "BillingCron")
    public BillingCron billingCron() {
        logger.trace("Creating an instance of BillingCron");

        return new BillingCron();
    }

    /*
     * Startup beans: End
     */

    /*
     * Lazy Singletons: Start
     */
    @Bean
    @Lazy
    public ModuleApplicationContextHolder applicationContextHolder() { // singleton bean
        logger.trace("Creating an instance of ApplicationContextHolder");

        return new ApplicationContextHolder();
    }

    @Bean
    @Lazy
    public MasterLockBean lockBean() { // singleton bean
        logger.trace("Creating an instance of MasterLockBean");

        return new MasterLockBean();
    }

    @Bean
    @Lazy
    public SchemaStore schemaStore() { // singleton bean
        logger.trace("Creating an instance of SchemaStore");

        return new SchemaStore();
    }

    @Bean
    @Lazy
    public TransactionLocking transactionLocking() { // singleton bean
        logger.trace("Creating an instance of TransactionLocking");

        return new TransactionLocking();
    }

    @Bean
    @Lazy
    public IndexCountStore indexCountStore() { // singleton bean
        logger.trace("Creating an instance of IndexCountStore");

        return new IndexCountStore();
    }

    @Bean
    @Lazy
    public OperationQueue operationQueue() { // singleton bean
        logger.trace("Creating an instance of OperationQueue");

        return new OperationQueue();
    }

    @Bean
    @Lazy
    public ActiveOperationStore activeOperationStore() { // singleton bean
        logger.trace("Creating an instance of ActiveOperationStore");

        return new ActiveOperationStore();
    }

    @Bean
    @Lazy
    public RowCountStore rowCountStore() { // singleton bean
        logger.trace("Creating an instance of RowCountStore");

        return new RowCountStore();
    }

    @Bean
    @Lazy
    public OperationLogger operationLogger() { // singleton bean
        logger.warn("An instance of OperationLogger was just created. This class is deprecated. Please work towards removing the usage of this class");

        return new OperationLogger();
    }

    @Bean
    @Lazy
    public OperationsFileStore operationsFileStore() { // singleton bean
        logger.trace("Creating an instance of OperationsFileStore");

        return new OperationsFileStore();
    }

    @Bean
    @Lazy
    public ProcedureStoreBean procedureStoreBean() { // singleton bean
        logger.trace("Creating an instance of ProcedureStoreBean");

        return new ProcedureStoreBean();
    }

    @Bean
    @Lazy
    public TriggerStoreBean triggerStoreBean() {
        logger.trace("creating an instance of TriggerStoreBean");

        return new TriggerStoreBean();
    }

    @Bean
    @Lazy
    public FilterStoreBean filterStoreBean() {
        logger.trace("creating an instance of FilterStoreBean");

        return new FilterStoreBean();
    }
    
    @Bean 
    @Lazy
    public InterpreterStoreBean interpreterStoreBean(){
        logger.trace("creating an instance of " + InterpreterStoreBean.class.getName());
        
        return new InterpreterStoreBean();
    }
    
    @Bean
    @Lazy
    public InterpreterExecutorBean InterpreterExecutorBean(){
        logger.trace("creating an instance of " + InterpreterExecutorBean.class.getName());
        
        return new InterpreterExecutorBean();
    }

    @Bean
    @Lazy
    public LoaderStore loaderStore() { // singleton bean
        logger.trace("Creating an instance of LoaderStore");

        return new LoaderStore();
    }

    @Bean
    @Lazy
    public ProximityNodesStore proximityNodesStore() { // singleton bean
        logger.trace("Creating an instance of ProximityNodesStore");

        return new ProximityNodesStore();
    }

    @Bean
    @Lazy
    public ClusterNodesStore clusterNodesStore() { // singleton bean
        logger.trace("Creating an clusterNodesStore of ClusterNodesStore");

        return new ClusterNodesStore();
    }

    @Bean
    @Lazy
    public NodeManager nodeManager() { // singleton bean
        logger.trace("Creating an instance of NodeManager");

        return new NodeManager();
    }

    @Bean
    @Lazy
    public SystemDBService systemDBService() { //singleton bean
        logger.trace("Creating an instance of SystemDBService");

        return new SystemDBService();
    }

    @Bean(name = "SecurityManagerBean")
    @Lazy
    public com.blobcity.db.security.SecurityManagerBean securityManagerBean() { //singleton bean
        logger.trace("Creating an instance of SecurityManager");

        return new com.blobcity.db.security.SecurityManagerBean();
    }

    @Bean
    @Lazy
    public TransactionStore transactionStore() { //singleton bean
        logger.trace("Creating an instance of TransactionStore");

        return new TransactionStore();
    }

    @Bean
    @Lazy
    public ConnectionStore connectionStore() { //singleton bean
        logger.trace("Creating an instance of ConnectionStore");

        return new ConnectionStore();
    }

    @Bean
    @Lazy
    public MasterStore masterStore() { //singleton bean
        logger.trace("Creating an instance of MasterStore");

        return new MasterStore();
    }

    @Bean
    @Lazy
    public QueryStore queryStore() { // singleton bean
        logger.trace("Creating an instance of QueryStore");

        return new QueryStoreBean();
    }

    @Bean
    @Lazy
    public DataCache dataCache() { // singleton bean
        logger.trace("Creating an instance of DataCache");

        return new DataCache();
    }

    @Bean
    @Lazy
    public CacheRules cacheRules() { // singleton bean
        logger.trace("Creating an instance of CacheRules");

        return new CacheRules();
    }

    @Bean
    public MemoryTableStore memoryTableStore() { // singleton bean
        logger.trace("Creating an instance of MemoryTableStore");

        return new MemoryTableStore();
    }
    
    @Bean
    public DataCubeStore dataCubeStore() { // singleton bean
        logger.trace("Creating an instance of DataCubeStore");

        return new DataCubeStore();
    }

    @Bean(name = "RequestStoreBean")
    public RequestStoreBean requestStore() { //singleton bean
        logger.trace("Creating an instance of RequestStore");

        return new RequestStoreBean();
    }

    @Bean(name = "MemCollectionStoreBean")
    public MemCollectionStoreBean memCollectionStoreBean() { //singleton bean
        logger.trace("Creating an instance of MemCollectionStoreBean");

        return new MemCollectionStoreBean();
    }
    
    @Bean
    @Lazy
    public JarManager jarManager(){
        logger.trace("Creating an instance of JarManager");
        
        return new JarManager();
    }
    
    @Bean
    @Lazy
    public MapReduceDriver mapReduceDriver(){
        logger.trace("Creating an instance of MapReduceDriver");
        
        return new MapReduceDriver();
    }
    
    @Bean
    @Lazy
    public MapReduceExecutor mapReduceExecutor(){
        logger.trace("Creating an instance of MapReduceExecutor");
        
        return new MapReduceExecutor();
    }
    
    @Bean
    @Lazy
    public MapReduceJobManager mapReduceJobManager(){
        logger.trace("Creating an instance of MapReduceJobManager");
        
        return new MapReduceJobManager();
    }
    
    @Bean
    @Lazy
    public MapReduceOutputImporter mapReduceOutputImporter(){
        logger.trace("Creating an instance of MapReduceOutputImporter");
        
        return new MapReduceOutputImporter();
    }
    
    @Bean
    @Lazy
    public MemorySearch memorySearch(){
        logger.trace("Creating an instance of MemorySearch");
        
        return new MemorySearch();
    }
    
    @Bean
    @Lazy
    public DataCubeManager DataCubeManager(){
        logger.trace("Creating an instance of DataCubeManager");
        
        return new DataCubeManager();
    }
    
    
    @Bean
    @Lazy
    public UserGroupManager userGroupManager(){
        logger.trace("Creating an instance of " + UserGroupManager.class.getSimpleName());
        
        return new UserGroupManager();
    }
    
    @Bean
    @Lazy
    public UserManager userManager(){
        logger.trace("Creating an instance of " + UserManager.class.getSimpleName());
        
        return new UserManager();
    }

    @Bean
    @Lazy
    public WatchServiceManager watchServiceManager(){
        logger.trace("Creating an instance of " + WatchServiceManager.class.getSimpleName());

        return new WatchServiceManager();
    }

    @Bean
    @Lazy
    public CollectionCommitLogWriter collectionCommitLogWriter(){
        logger.trace("Creating an instance of " + CollectionCommitLogWriter.class.getSimpleName());

        return new CollectionCommitLogWriter();
    }

    @Bean
    @Lazy
    public ProcessorStore processorStore() {
        logger.trace("Creating an instance of " + ProcessorStore.class.getSimpleName());

        return new ProcessorStore();
    }

    @Bean
    @Lazy
    public TransientStore transientStore() {
        logger.trace("Creating an instance of " + TransientStore.class.getSimpleName());

        return new TransientStore();
    }

    @Bean
    @Lazy
    public CentralCommitLogWriter centralCommitLogWriter() {
        logger.trace("Creating an instance of " + CentralCommitLogWriter.class.getSimpleName());

        return new CentralCommitLogWriter();
    }

    @Bean
    @Lazy
    public FtpServerManager ftpServerManager() {
        logger.trace("Creating an instance of " + FtpServerManager.class.getSimpleName());

        return new FtpServerManager();
    }

    @Bean
    @Lazy
    public WebServiceStore webServiceStore() {
        logger.trace("Creating an instance of " + WebServiceStore.class.getSimpleName());

        return new WebServiceStore();
    }

    @Bean
    @Lazy
    public TableauConfig tableauConfig() {
        logger.trace("Creating an instance of " + TableauConfig.class.getSimpleName());

        return new TableauConfigBean();
    }

    @Bean
    @Lazy
    public OnDiskBtreeIndexCache onDiskBtreeIndexCache() {
        logger.trace("Creating an instance of " + OnDiskBtreeIndexCache.class.getSimpleName());

        return new OnDiskBtreeIndexCache();
    }

    @Bean
    @Lazy
    public QueryResultCache queryResultCache() {
        logger.trace("Creating an instance of " + QueryResultCache.class.getSimpleName());

        return new QueryResultCache();
    }

    @Bean
    @Lazy
    public ExportProcedureStore exportProcedureStore() {
        logger.trace("Creating an instance of " + ExportProcedureStore.class.getSimpleName());

        return new ExportProcedureStore();
    }

    /*
     * Lazy Singletons: End
     */

    /*
     * Prototypes: Start
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SQLExecutorBean sqlExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of SQLExecutorBean");

        return new SQLExecutorBean();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BSqlDatastoreManager databaseManager() { // stateless/stateful bean
        logger.trace("Creating an instance of BSqlDatabaseManager");

        return new BSqlDatastoreManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BSqlFileManager fileManager() { // stateless/stateful bean
        logger.trace("Creating an instance of BSqlFileManager");

        return new BSqlFileManager();
    }
    
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BSqlMemoryManagerOld memoryManagerOld() { // stateless/stateful bean
        logger.trace("Creating an instance of BSqlMemoryManagerOld");

        return new BSqlMemoryManagerOld();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BSqlCollectionManager tableManager() { // stateless/stateful bean
        logger.trace("Creating an instance of BSqlTableManager");

        return new BSqlCollectionManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BSqlIndexManager indexManager() { // stateless/stateful bean
        logger.trace("Creating an instance of BSqlIndexManager");

        return new BSqlIndexManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OperationsManager operationsManager() { // stateless/stateful bean
        logger.trace("Creating an instance of OperationsManager");

        return new OperationsManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BSqlDataManager dataManager() { // stateless/stateful bean
        logger.trace("Creating an instance of BSqlDataManager");

        return new BSqlDataManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ClusterDataManager clusterDataManager() { // stateless/stateful bean
        logger.trace("Creating an instance of " + ClusterDataManager.class.getSimpleName());

        return new ClusterDataManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SelectExecutor selectExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of SelectExecutor");

        return new SelectExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public UpdateExecutor updateExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of UpdateExecutor");

        return new UpdateExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DeleteExecutor deleteExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of DeleteExecutor");

        return new DeleteExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public CreateTableExecutor createTableExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of CreateTableExecutor");

        return new CreateTableExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public AlterTableExecutor alterTableExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of AlterTableExecutor");

        return new AlterTableExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DropTableExecutor dropTableExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of DropTableExecutor");

        return new DropTableExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public CreateSchemaExecutor createSchemaExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of CreateSchemaExecutor");

        return new CreateSchemaExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SchemaManager schemaManager() { // stateless/stateful bean
        logger.trace("Creating an instance of SchemaManager");

        return new SchemaManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public GlobalLiveManager globalLiveManager() { // stateless/stateful bean
        logger.trace("Creating an instance of GlobalLiveManager");

        return new GlobalLiveManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public GlobalLiveStore globalLiveStore() { // stateless/stateful bean
        logger.trace("Creating an instance of GlobalLiveStore");

        return new GlobalLiveStore();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public IndexFactory indexFactory() { // stateless/stateful bean
        logger.trace("Creating an instance of IndexFactory");

        return new IndexFactory();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OperationExecutor operationExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of OperationExecutor");

        return new OperationExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OperationFactory operationFactory() { // stateless/stateful bean
        logger.trace("Creating an instance of OperationFactory");

        return new OperationFactory();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public RecordLockBean recordLockBean() { // stateless/stateful bean
        logger.trace("Creating an instance of RecordLockBean");

        return new RecordLockBean();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public TypeConverterFactory typeConverterFactory() { // stateless/stateful bean
        logger.trace("Creating an instance of TypeConverterFactory");

        return new TypeConverterFactory();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public RowCountManager rowCountManager() { // stateless/stateful bean
        logger.trace("Creating an instance of RowCountManager");

        return new RowCountManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ManifestParserBean manifestParserBean() { // stateless/stateful bean
        logger.trace("Creating an instance of ManifestParserBean");

        return new ManifestParserBean();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ProcedureExecutorBean procedureExecutorBean() { // stateless/stateful bean
        logger.trace("Creating an instance of procedureExecutorBean");

        return new ProcedureExecutorBean();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public TriggerExecutorBean triggerExecutorBean() {
        logger.trace("creating an instance of TriggerExecutorBean");

        return new TriggerExecutorBean();
    }

    @Bean
    @Lazy
    //TODO : check what to use prototype or lazy
    public FilterExecutorBean filterExecutorBean() {
        logger.trace("creating an instance of FilterExecutorBean");

        return new FilterExecutorBean();
    }

    @Bean
    @Lazy
    public FilterParallelExecutor filterParallelExecutor() {
        logger.trace("creating instance of filterParallelExecutor");

        return new FilterParallelExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ThreadRun threadRun() {
        return new ThreadRun();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public QueryExecutorImpl queryExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of QueryExecutorImpl");

        return new QueryExecutorImpl();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ConnectionManager connectionManager() { // stateless/stateful bean
        logger.trace("Creating an instance of ConnectionManager");

        return new ConnectionManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BQuerySelectExecutor bQuerySelectExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of BQuerySelectExecutor");

        return new BQuerySelectExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BQueryInsertExecutor bQueryInsertExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of BQueryInsertExecutor");

        return new BQueryInsertExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BQueryUpdateExecutor bQueryUpdateExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of BQueryUpdateExecutor");

        return new BQueryUpdateExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BQueryRemoveExecutor bQueryRemoveExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of BQueryRemoveExecutor");

        return new BQueryRemoveExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public CodeExecutor codeExecutor() {
        logger.trace("creating an instance of CodeExecutor");

        return new CodeExecutor();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public CodeLoader codeLoader() {
        logger.trace("creating an instance of codeLoader");

        return new CodeLoader();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public FileWatchService fileWatchService(){
        logger.trace("Creating an instance of " + FileWatchService.class.getSimpleName());

        return new FileWatchService();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public FileTailListener fileTailListener(){
        logger.trace("Creating an instance of " + FileTailListener.class.getSimpleName());

        return new FileTailListener();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public FolderWatchService folderWatchService(){
        logger.trace("Creating an instance of " + FolderWatchService.class.getSimpleName());

        return new FolderWatchService();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public HookableEvent hookableEvent(){
        logger.trace("Creating an instance of " + HookableEventBean.class.getSimpleName());

        return new HookableEventBean();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public HookableTransaction hookeableTransaction(){
        logger.trace("Creating an instance of " + HookableTransactionBean.class.getSimpleName());

        return new HookableTransactionBean();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public TableauSiteManager tableauSiteManager(){
        logger.trace("Creating an instance of " + TableauSiteManager.class.getSimpleName());

        return new TableauSiteManagerBean();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public InternalQueryBean internalQueryBean(){
        logger.trace("Creating an instance of " + InternalQueryBean.class.getSimpleName());

        return new InternalQueryBean();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public TableauPublishManager tableauPublishManager(){
        logger.trace("Creating an instance of " + TableauPublishManager.class.getSimpleName());

        return new TableauPublishManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public TableauTdeManager tableauTdeManager(){
        logger.trace("Creating an instance of " + TableauTdeManager.class.getSimpleName());

        return new TableauTdeManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BSqlMemoryManager bSqlMemoryManager(){
        logger.trace("Creating an instance of " + BSqlMemoryManager.class.getSimpleName());

        return new BSqlMemoryManager();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskAggregateHandling onDiskAggregateHandling(){
        logger.trace("Creating an instance of " + OnDiskAggregateHandling.class.getSimpleName());

        return new OnDiskAggregateHandling();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskAvgHandling onDiskAvgHandling(){
        logger.trace("Creating an instance of " + OnDiskAvgHandling.class.getSimpleName());

        return new OnDiskAvgHandling();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskCountHandling onDiskCountHandling(){
        logger.trace("Creating an instance of " + OnDiskCountHandling.class.getSimpleName());

        return new OnDiskCountHandling();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskDistinctHandling onDiskDistinctHandling(){
        logger.trace("Creating an instance of " + OnDiskDistinctHandling.class.getSimpleName());

        return new OnDiskDistinctHandling();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskGroupByHandling onDiskGroupByHandling(){
        logger.trace("Creating an instance of " + OnDiskGroupByHandling.class.getSimpleName());

        return new OnDiskGroupByHandling();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskMaxHandling onDiskMaxHandling(){
        logger.trace("Creating an instance of " + OnDiskMaxHandling.class.getSimpleName());

        return new OnDiskMaxHandling();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskMinHandling onDiskMinHandling(){
        logger.trace("Creating an instance of " + OnDiskMinHandling.class.getSimpleName());

        return new OnDiskMinHandling();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskSumHandling onDiskSumHandling(){
        logger.trace("Creating an instance of " + OnDiskSumHandling.class.getSimpleName());

        return new OnDiskSumHandling();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskWhereHandling onDiskWhereHandling(){
        logger.trace("Creating an instance of " + OnDiskWhereHandling.class.getSimpleName());

        return new OnDiskWhereHandling();
    }

    /*
     * Prototypes: End
     */

    /*
     * Named Prototypes: Start
     */
    // IndexingStrategy beans: Start
    @Bean(name = "OnDiskBTreeIndex")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskBTreeIndex bTreeIndex() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of OnDiskBTreeIndex");

        return new OnDiskBTreeIndex();
    }

    @Bean(name = "OnDiskHashedIndex")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskHashedIndex hashedIndex() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of OnDiskHashedIndex");

        return new OnDiskHashedIndex();
    }

    @Bean(name = "OnDiskUniqueIndex")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnDiskUniqueIndex uniqueIndex() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of OnDiskUniqueIndex");

        return new OnDiskUniqueIndex();
    }
    // IndexingStrategy beans: End

    // Operable beans: Start
    @Bean(name = "IndexOperation")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public IndexOperation indexOperation() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of IndexOperation");

        return new IndexOperation();
    }

    @Bean(name = "CsvImporter")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public CsvImporter csvImporter() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of CsvImporter");

        return new CsvImporter();
    }

    @Bean(name = "CsvExporter")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public CsvExporter csvExporter() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of CsvExporter");

        return new CsvExporter();
    }
    // Operable beans: End

    // TypeConverter beans: Start
    @Bean(name = "IntType")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public IntConverter intConverter() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of IntConverter");

        return new IntConverter();
    }

    @Bean(name = "LongType")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public LongConverter longConverter() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of LongConverter");

        return new LongConverter();
    }

    @Bean(name = "FloatType")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public FloatConverter floatConverter() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of FloatConverter");

        return new FloatConverter();
    }

    @Bean(name = "DoubleType")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DoubleConverter doubleConverter() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of DoubleConverter");

        return new DoubleConverter();
    }

    @Bean(name = "StringType")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public StringConverter stringConverter() { // stateless/stateful factory patterned bean
        logger.trace("Creating an instance of StringConverter");

        return new StringConverter();
    }

    @Bean(name = "BQueryExecutorBean")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BQueryExecutorBean bQueryExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of BQueryExecutorBean");

        return new BQueryExecutorBean();
    }

    @Bean(name = "BQueryAdminBean")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BQueryAdminBean bQueryAdminBean() { // stateless/stateful bean
        logger.trace("Creating an instance of BQueryAdminBean");

        return new BQueryAdminBean();
    }

    @Bean(name = "ConsoleExecutorBean")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ConsoleExecutorBean consoleExecutorBean() { // stateless/stateful bean
        logger.trace("Creating an instance of ConsoleExecutorBean");

        return new ConsoleExecutorBean();
    }

    @Bean(name = "AdapterExecutorBean")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public AdapterExecutorBean adapterExecutorBean() { // stateless/stateful bean
        logger.trace("Creating an instance of AdapterExecutorBean");

        return new AdapterExecutorBean();
    }

    @Bean(name = "ClusterMessaging")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ClusterMessaging clusterMessaging() { // stateless/stateful bean
        logger.trace("Creating an instance of ClusterMessaging");

        return new ClusterMessaging();
    }

    // TypeConverter beans: End
    @Bean(name = "VersionUpgradeFactory")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public VersionUpgradeFactory versionUpgradeFactory() { // stateless/stateful bean
        logger.trace("Creating an instance of VersionUpgradeFactory");

        return new VersionUpgradeFactory();
    }

    @Bean(name = "Version1to2")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public Version1to2 version1to2() { // stateless/stateful bean
        logger.trace("Creating an instance of Version1to2");

        return new Version1to2();
    }

    @Bean(name = "Version2to3")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public Version2to3 version2to3() { // stateless/stateful bean
        logger.trace("Creating an instance of Version2to3");

        return new Version2to3();
    }
    
    @Bean(name = "Version3to4")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public Version3to4 version3to4() { // stateless/stateful bean
        logger.trace("Creating an instance of " + Version3to4.class.getSimpleName());

        return new Version3to4();
    }

    @Bean(name = "HomeReportingBean")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public HomeReportingBean homeReportingBean() { // stateless/stateful bean
        logger.trace("Creating an instance of HomeReportingBean");

        return new HomeReportingBean();
    }

    @Bean(name = "DDLStatement")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DDLStatement ddlStatementBean() { // stateless/stateful bean
        logger.trace("Creating an instance of DDLStatement");

        return new DDLStatement();
    }

    @Bean(name = "RequestHandlingBean")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public RequestHandlingBean requestHandlingBean() { // stateless/stateful bean
        logger.trace("Creating an instance of RequestHandlingBean");

        return new RequestHandlingBean();
    }

    @Bean(name = "FtpServiceManager")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public FtpServiceManager ftpServiceManager() { // stateless/stateful bean
        logger.trace("Creating an instance of FtpServiceManager");

        return new FtpServiceManager();
    }

    @Bean(name = "WebServiceExecutor")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public WebServiceExecutor webServiceExecutor() { // stateless/stateful bean
        logger.trace("Creating an instance of WebServiceExecutor");

        return new WebServiceExecutorBean();
    }

    @Bean(name = "NLPImplementation")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public NLP NLP() {
        logger.trace("Creating an instance of NLPImplementation");

        return new NLP();
    }

    @Bean(name = "ExportServiceRouter")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ExportServiceRouter exportServiceRouter() {
        logger.trace("Creating an instance of ExportServiceRouter");

        return new ExportServiceRouterBean();
    }

    @Bean(name = "SelectActivityLog")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SelectActivityLog selectActivityLog() {
        logger.trace("Creating an instance of SelectActivityLog");

        return new SelectActivityLog();
    }

    @Bean(name = "ApiKeyManager")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ApiKeyManager apiKeyManager() {
        logger.trace("Creating an instance of ApiKeyManager");

        return new ApiKeyManager();
    }

    /*
     * Named Prototypes: End
     */
}
