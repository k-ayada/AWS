from __future__          import print_function

import sys,logging,boto3,json

from collections         import OrderedDict

from pyspark             import SparkConf
from pyspark.sql         import SparkSession
from pyspark.sql         import types  as dfTypes

class InvalidRunOptions(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)
class PyUtilFunc:
    def __init__(self, **kwargs):
        self.log  = kwargs['log']
    def pritifyDict(self,d) :
        return json.dumps(d, indent=4, sort_keys=True, default=str)
class AWS:
    def __init__(self, **kwargs):
        self.log  = kwargs['log']
        self.s3   = self.S3(log=self.log)
        self.glue = self.GLUE(log=self.log)
        self.ssm = self.SSM(log=self.log)

    class SSM:
        def __init__(self, **kwargs):
            self.ssm   = boto3.client('ssm')
            self.log     = kwargs['log']
        def get(self,keyIn):
            '''
            Gets the value from AWS Parameter Store
            '''
            key = keyIn[4:] if keyIn.startswith("ssm:/") else keyIn
            self.log.info("Getting the value for %s" % (key))
            val = self.ssm.get_parameter(Name=key, WithDecryption=True)['Parameter']['Value']
            return val
    class S3:
        def __init__(self, **kwargs):
            self.s3Res   = boto3.resource('s3')
            self.s3      = boto3.client('s3')
            self.log     = kwargs['log']

        def getBucketNKeyTuple(self,uriStr: str) -> (str,str)  :
            splt = uriStr.replace("s3://","").split("/")
            bkt = splt.pop(0)
            key = "/".join(splt)
            return (bkt,key)

        def deleteS3Object(self,bucket, key):
            if not key.endswith("/")  :
                key = "%s/" % (key)

            keyLst =  self.s3Res.Bucket(bucket).objects.filter(Prefix="%s" % (key))
            for i,k in enumerate(keyLst):
                self.log.debug("%3d Deleting s3 Object: %s/%s" % (i,bucket,k))
                self.s3.delete_object(Bucket=k.bucket_name,Key=k.key)
    class GLUE:
        def __init__(self, **kwargs):
            self.ssm = boto3.client('ssm')
            self.log = kwargs['log']

class Ingest:
    def __init__(self):
        self.log     = logging.getLogger(__name__)
        self.parms   = OrderedDict()
        self.mainSQL = OrderedDict()
        self.spark   = None
        self.pyu     = PyUtilFunc(log=self.log)
        self.aws     = AWS(log=self.log)
        self.s3      = self.aws.s3
        self.ssm     = self.aws.ssm
        self.glue    = self.aws.glue

    def start(self,argv):
        '''
        Driver function for handling the ingest
        '''
        self.prseParms(argv)
        self.setupLogger()
        self.validateArgs()
        self.handleSSM()
        self.reportRunTimeParms()
        self.startSparkSession("Ingest-" +  self.parms["--dstDB"] + "." + self.parms["--dstTable"])
        self.validateDestTable()
        self.buidMainSQL()
        jdbcDF =  self.readFromRDBMS()
        jdbcDFName = "jdbcDf_" + self.parms["--dstDB"] + "_" + self.parms["--dstTable"]
        jdbcDF.createOrReplaceTempView(jdbcDFName)
        jdbcDF.printSchema()
        xferSQL = """
        select {0}
        from {1}
        DISTRIBUTE BY {2}
        """.format(self.reSetColumnOrder(jdbcDF.schema.names),
                jdbcDFName,
                self.parms["--writePartitionCol"]
                )
        if self.parms["--writeMode"] == 'overwrite':
            self.createOrReplaceTable(xferSQL)
        else :
            self.insertIntoTable(jdbcDF,xferSQL)

    def prseParms(self,argv):
        '''
            Convert the program argumens in list to dict
            For key-value bsed parms use format --key value
            For flag based parms you  dont prepend '--'
            Example `argsToDict(['--key' , 'value' , '-flag'])`
            Retuns
            `{'-key' : 'value' , '-flag': '-flag' }`
        '''
        i = 1
        while i <= len(argv) -1:
            if ('--' == argv[i][0:2]):
                self.parms[argv[i]] = argv[i+1]
                i = i + 1
            else:
                self.parms[argv[i]] = argv[i]
            i = i + 1

    def setupLogger(self):
        '''
        Inits the python logging object
        '''
        handler   = logging.StreamHandler(sys.stdout)
        dfmt = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter('%(levelname)s %(asctime)s %(module)s.%(funcName)s[%(lineno)03d] %(message)s',dfmt)
        handler.setFormatter(formatter)
        if '--logLevel' not in self.parms :
            self.parms['--logLevel'] = 'i'

        lvl = self.parms['--logLevel']
        if   lvl == 'i': self.log.setLevel(logging.INFO)
        elif lvl == 'w': self.log.setLevel(logging.WARN)
        elif lvl == 'd': self.log.setLevel(logging.DEBUG)
        elif lvl == 'e': self.log.setLevel(logging.ERROR)
        elif lvl == 'c': self.log.setLevel(logging.CRITICAL)
        else           : self.log.setLevel(logging.INFO)

        self.log.addHandler(handler)

    def validateArgs(self) :
        '''
        Validates if below parms are received.
        Mandetory: `--srcDB`, `--srcTable`, `--user`, `--password`, `--jdbcUrl` and `--jdbcDriver`
        Below parms will be defaluted.
        `--writeMode`         `'overwrite' if '-fullLoad' else 'append'` \n
        `--writePartitionCol` `''` \n
        `--logLevel`          `'d'`\n
        `--outFlFmt`          `'parquet'`\n
        `--jdbcReadSplits`    `10`\n
        `--dstDB`             `--srcDB`\n
        `--dstTable`          `--srcTable`\n
        '''
        issue=False
        if '--srcDB' not in self.parms :
            self.log.error('Please pass a valid value for --srcDB')
            issue=True
        if '--srcTable' not in self.parms :
            self.log.error('Please pass a valid value for --srcTable')
            issue=True
        if '--user' not in self.parms :
            self.log.error('Please pass a valid value for --user')
            issue=True
        if '--password' not in self.parms :
            self.log.error('Please pass a valid value for --password')
            issue=True
        if '--jdbcUrl' not in self.parms :
            self.log.error('Please pass a valid value for --jdbcUrl')
            issue=True

        if '--jdbcDriver' not in self.parms :
            self.log.error('Please pass a valid value for --jdbcDriver')
            issue=True

        if issue:
            self.log.error('Existting the program as one of the mandetory argument is missing.')
            exit(1)

        self.parms['--dstDB']    = self.parms.get('--dstDB'   , self.parms['--srcDB']   ).upper()
        self.parms['--dstTable'] = self.parms.get('--dstTable', self.parms['--srcTable']).upper()

        if "--srcTZ"          not in self.parms : self.parms['--srcTZ']          = 'UTC'
        if '--outFlFmt'       not in self.parms : self.parms['--outFlFmt']       = 'parquet'
        if '--jdbcReadSplits' not in self.parms : self.parms['--jdbcReadSplits'] = 10
        if '--writeMode'      not in self.parms : self.parms['--writeMode']      =      'overwrite' \
                                                                        if   '-fullLoad' in self.parms \
                                                                        else 'append'

        if self.parms['--outFlFmt'] not in ['parquet', 'orc', 'csv']:
            self.log.error('Currently we only support parquet, orc and csv')
            exit(1)


        if '--writePartitionCol'  in self.parms :
            self.parms['--writePartitionCol'] = self.parms['--writePartitionCol'].upper()

    def handleSSM(self):
        ''''
        if one of the parameter entry point to ssm (by prepending 'ssm:'). Calls ssm to retrive the value
        '''
        for key in self.parms:
            if self.parms[key].startswith('ssm:/'):
                self.parms[key] = self.ssm.get(self.parms[key])

    def reportRunTimeParms(self):
        '''
        log the input parms
        '''
        msg = ''
        for i, key in enumerate(self.parms):
            if key == '--password' and ( not self.parms[key].startswith('ssm:/')):
                msg += "\t %2d. %20s -> ********\n" %(i, key )
            else :
                msg +="\t %2d. %20s -> %s\n" %(i, key, self.parms[key] )

        self.log.info("Starting the processing with below parms,\n {}\n\n".format(msg))

    def startSparkSession(self,appName):
        #https://spark.apache.org/docs/latest/cloud-integration.html
        hmConf = {
            "spark.executor.pyspark.memory"                  : "512m",
            "spark.debug.maxToStringFields"                  : "5000",
            "spark.rps.askTimeout"                           : "1200",
            "spark.network.timeout"                          : "1200",

            "spark.maxRemoteBlockSizeFetchToMem"             : "512m",
            "spark.broadcast.blockSize"                      : "16m",
            "spark.broadcast.compress"                       : "true",
            "spark.rdd.compress"                             : "true",
            "spark.io.compression.codec"                     : "org.apache.spark.io.SnappyCompressionCodec",

            "spark.kryo.unsafe"                              : "true",
            "spark.serializer"                               : "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer"                    : "10240",
            "spark.kryoserializer.buffer.max"                : "2040m",
            "hive.exec.dynamic.partition"                    : "true",
            "hive.exec.dynamic.partition.mode"               : "nonstrict",
            "hive.warehouse.data.skiptrash"                  : "true",

            "spark.sql.hive.metastorePartitionPruning"       : "true",

            "fs.s3a.fast.upload"                             : "true",
            "fs.s3.buffer.dir"                               : "/mnt/tmp/s3",

            "spark.sql.broadcastTimeout"                                    : "1200",
            "spark.sql.sources.partitionOverwriteMode"                      : "dynamic",

            "spark.sql.orc.filterPushdown"                                  : "true",
            "spark.sql.orc.splits.include.file.footer"                      : "true",
            "spark.sql.orc.cache.stripe.details.size"                       : "1000",

            "spark.hadoop.parquet.enable.summary-metadata"                  : "false",
            "spark.sql.parquet.mergeSchema"                                 : "false",
            "spark.sql.parquet.filterPushdown"                              : "true",
            "spark.sql.parquet.fs.optimized.committer.optimization-enabled" : "true",
            "spark.sql.parquet.output.committer.class"                      : "com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter",

            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version"       :"2",
            "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored": "true"

        }
        sparkConf = SparkConf()

        for (k,v) in hmConf.items():
            sparkConf.set(k,v)

        self.spark = SparkSession \
                .builder \
                .config(conf=sparkConf) \
                .appName(appName or "PySparkApp") \
                .enableHiveSupport() \
                .getOrCreate()

        sc =self.spark.sparkContext
        sc.setSystemProperty("com.amazonaws.services.s3.enableV4",  "true")
        sc.setSystemProperty("com.amazonaws.services.s3.enforceV4", "true")
        sc.setLogLevel(self.parms.get("--sparklogLvl", "INFO"))

        #from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher
        #provider = InstanceMetadataProvider(iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2))
        #creds = provider.load()
        #session = boto3.Session(region_name=parms.get("--awsRegion", "us-east-1"))
        #creds   = session.get_credentials().get_frozen_credentials()
        #log.debug("boto2: {}".format(creds))

        hdpCnf = sc._jsc.hadoopConfiguration()

        hdpCnfhm = {
            "io.file.buffer.size"                             : "65536",
            "mapreduce.fileoutputcommitter.algorithm.version" : "2",
            #"fs.s3a.access.key"                               : creds.access_key,
            #"fs.s3a.secret.key"                               : creds.secret_key,
            #"fs.s3a.server-side-encryption-algorithm"         : "SSE-KMS",
            #"fs.s3.enableServerSideEncryption"                : "true",
            #"fs.s3.impl"                                      : "org.apache.hadoop.fs.s3a.S3AFileSystem",
            #"fs.s3a.impl"                                     : "org.apache.hadoop.fs.s3a.S3AFileSystem",
            #"fs.s3a.endpoint"                                 : "s3.%s.amazonaws.com" % (parms.get("--awsRegion", "us-east-1"))
        }

        for (k,v) in hdpCnfhm.items():
            hdpCnf.set(k,v)

        msg = ""
        for k in sc._conf.getAll():
            msg += "\n\t'%s' -> '%s'" % (k[0], k[1])

        self.log.info("Initiated SparkSesion with below confs,{}".format(msg))

    def validateDestTable(self):
        tabeFound =self.spark.catalog._jcatalog.tableExists(self.parms['--dstDB'], self.parms['--dstTable'])

        if tabeFound and self.parms['--writeMode'] == 'overwrite':
            loc = self.spark.sql("describe formatted {0}.{1}".format(self.parms['--dstDB'], self.parms['--dstTable'])) \
                      .filter("col_name = 'Location'").collect()[0][1]
            (bkt,key) = self.s3.getBucketNKeyTuple(loc)
            self.log.info("dropping the table as '--writeMode' == 'overwrite'. Table: {0}.{1}" \
                    .format(self.parms["--dstDB"],self.parms["--dstTable"]))
            self.log.info("Deleting the backend file : {0}/{1}".format(bkt,key))
            self.s3.deleteS3Object(bkt,key)
            self.spark.sql("drop table if exists {0}.{1}".format(self.parms["--dstDB"],self.parms["--dstTable"]))

        elif  (not tabeFound) and self.parms['--writeMode'] != 'overwrite':
            self.log.warn("Destination table not found and the --writeMode is '{0}'. Re-stetting the --writeMode as 'overwrite'"\
                          .format(self.parms['--writeMode']))
            self.parms['--writeMode'] = 'overwrite'
        else:
            tbFmt = self.spark.sql("describe formatted {0}.{1}".format(self.parms['--dstDB'], self.parms['--dstTable'])) \
                .filter("col_name = 'Provider'").collect()[0][1]

            if tbFmt != self.parms['--outFlFmt']:
                self.log.warn("format of the table {0}.{1} '{2}' does not match the value passed for '--outFlFmt' '{3}'" \
                        .format(self.parms['--dstDB'], self.parms['--dstTable'], self.parms['--outFlFmt'], tbFmt ))

    def buidMainSQL(self):
        '''
            Creatses the main sql details using below parms,
            Mandetory : --srcDB, --srcTable
            Optional  : --where, --deltaOn, --deltaOnLB, --deltaOnUB
        '''
        self.mainSQL['select'] = "select  /*+ ALL_ROWS */ {0} ".format( self.parms.get('--selectCols', '*'))
        self.mainSQL['from']   = "  from {0}.{1}".format(self.parms['--srcDB'],self.parms['--srcTable'])
        self.mainSQL['where']  = self.parms.get('--where', None)

        if '--deltaOn' in self.parms:
            self.mainSQL['deltaOn']  = self.parms['--deltaOn']
            if '--deltaUBVal' in self.parms:
                self.mainSQL['deltaUBVal']  = self.parms['--deltaUBVal']
                if '--deltaUBCond' not in self.parms:
                    self.log.warn("--deltaUBCond not received. Using '<='")
                    self.mainSQL['deltaUBCond'] = '<='

            if self.parms.get('--deltaLBVal', 'fromSrc') != 'fromSrc':
                self.mainSQL['deltaLBVal']  = self.parms['--deltaLBVal']
            else:
                if self.spark.catalog._jcatalog.tableExists(self.parms['--dstDB'], self.parms['--dstTable']):
                    val  = self.getCurrentMaxFromDest()
                    self.log.warn("--deltaLBVal value derived from the destination table (current max): {0}".format(val))
                    if val != None:
                        self.mainSQL['deltaLBVal'] = val
                else:
                    raise InvalidRunOptions("For delta load, we need value for '--deltaOnLB' or the destination table should be present")

            if '--deltaLBCond' not in self.parms and self.mainSQL.get('deltaLBVal',None)  != None:
                self.log.warn("--deltaLBCond not received. Using '>='")
                self.mainSQL['deltaLBCond'] = '>='

    def getCurrentMaxFromDest(self):
        mxLimit = ""
        if  'deltaUBVal' in self.mainSQL:
            mxLimit = " where {0} {1} {2}"\
                      .format(self.mainSQL['deltaOn'],
                              self.mainSQL['deltaUBCond'],
                              self.mainSQL['deltaUBVal'])

        mxSQL = """
        select max({0}) as MX_VAL
          from {1}.{2}
         {3}
        """.format(self.parms['--deltaOn'],
                   self.parms['--dstDB'],
                   self.parms['--dstTable'],
                   mxLimit)
        self.log.info("Running the SQL to get the current max value in Hive,\n{}".format(mxSQL))
        df_max = self.spark.sql(mxSQL)
        df_max.createOrReplaceTempView("df_max")
        dt = type(df_max.schema.fields[0].dataType)

        if dt == 'timestamp':
            if self.parms["--srcTZ"] != "UTC" :
                col = "date_format(from_utc_timestamp(MX_VAL, '{0}'), 'yyyy-MM-dd HH:mm:ss') as MX, MX_VAL as inUTC"\
                      .format(self.parms['--srcTZ'])
            else:
                col = "date_format(MX_VAL, 'yyyy-MM-dd HH:mm:ss') as MX"
        elif dt == 'date':
            if self.parms["--srcTZ"] != "UTC" :
                col = "date_format(from_utc_timestamp(cast(MX_VAL as TimestampType) , '{0}'), 'yyyy-MM-dd') as MX, MX_VAL as inUTC"\
                      .format(self.parms['--deltaOn'], self.parms['--srcTZ'])
            else:
                col = "date_format(MX_VAL, 'yyyy-MM-dd') as MX"
        else:
            col = "cast(MX_VAL as string) as MX"

        sqlStr ="""
              select {0}
                from df_max
        """.format(col)

        dfRow = self.spark.sql(sqlStr).collect()
        strMaxVal = dfRow[0]['MX']

        self.log.info("Calculated max({0}) in {1}.{2} : {3}  ( Row : {4})"\
                .format(self.parms['--deltaOn'],
                        self.parms['--dstDB'],
                        self.parms['--dstTable'],
                        strMaxVal,
                        dfRow
                        )
                )
        return strMaxVal

    def readFromRDBMS(self):
        whereOrAnd = "where"
        where = ""
        if self.mainSQL['where'] != None :
            where = " where {}".format(self.mainSQL['where'])
            whereOrAnd = "and"

        (deltaLB,deltaUB) = ("","")

        if 'deltaOn' in self.mainSQL:
            if 'deltaLBVal' in self.mainSQL :
                deltaLB = " {0} {1} {2} {3}" \
                          .format(whereOrAnd,
                                  self.mainSQL['deltaOn'],
                                  self.mainSQL.get('deltaLBCond', '>='),
                                  self.mainSQL['deltaLBVal'])
                whereOrAnd = " and"
            if 'deltaUBVal' in self.mainSQL :
                deltaUB = " {0} {1} {2} {3}" \
                          .format(whereOrAnd,
                                  self.mainSQL['deltaOn'],
                                  self.mainSQL.get('deltaUBCond', '<='),
                                  self.mainSQL['deltaUBVal'])
        fltr = """
        {0}
        {1}
        {2}
        """.format(where,deltaLB,deltaUB)

        self.mainSQL['filter'] = " " + fltr.strip()

        mSQLStr = """
        {0}
        {1}
        {2}
        """.format(self.mainSQL['select']
                ,self.mainSQL['from']
                ,self.mainSQL['filter']
        )
        self.log.info("Built main SQL using the below options,\n %s" %  (self.pyu.pritifyDict(self.mainSQL)) )
        self.log.info("Main SQL,\n %s" %  ( mSQLStr ))
        jdbcReader = self.spark.read \
                        .format("jdbc") \
                        .option("fetchsize" , "10000") \
                        .option("user"      , self.parms["--user"]) \
                        .option("password"  , self.parms["--password"]) \
                        .option("driver"    , self.parms["--jdbcDriver"] ) \
                        .option("url"       , self.parms["--jdbcUrl"]) \
                        .option("dbtable"   ," ( {} ) x".format(mSQLStr))

        jdbcReader = self.updateFetchPartionInfo(jdbcReader)

        jdbcDF = jdbcReader.load().persist() \
            if "-cacheRecs" in self.parms \
            else jdbcReader.load()

        if '--newCols' not in self.parms :
            return jdbcDF
        else:
            jdbcDF.createOrReplaceTempView("jdbcDF")
            withNewCol = """
            select jdbcDF.*,
                   {0}
                from jdbcDF
            """.format(self.parms["--newCols"] )
            self.log.info("SQL statement for building transformed DF\n{}".format(withNewCol))
            return self.spark.sql(withNewCol)

    def updateFetchPartionInfo(self,jdbcReader):
        if "--fetchPartitionCol" in self.parms :
            mn,mx=None,None
            if "--lowerBound" in self.parms:
                mn =  int(self.parms["--lowerBound"])
                self.log.info("Using the provided lowerBound: {0}".format(mn))
            if "--upperBound" in self.parms:
                mx =  int(self.parms["--upperBound"])
                self.log.info("Using the provided upperBound: {0}".format(mx))

            if mn == None and mx == None :
                (mn,mx) = self.getMinMax()
                self.log.info("Using the derieved --lowerBound {0} --upperBound {1}".format(mn,mx))
            elif mn == None:
                mn = self.getMinOrMax(typ='min')
                self.log.info("Using the derieved --lowerBound {0}".format(mn))
            elif mx == None:
                mx = self.getMinOrMax(typ='max')
                self.log.info("Using the derieved --upperBound {0}".format(mx))

        if ( mn == None and mx == None) :
            self.log.info("Not able to derieve the lower and upper bounds. Falling back to single threaded JDBC read.")
            return jdbcReader
        else:
            return jdbcReader.option("partitionColumn" ,self.parms["--fetchPartitionCol"]) \
                            .option("numPartitions"   ,self.parms["--jdbcReadSplits"])\
                            .option("lowerBound"      ,str(mn))\
                            .option("upperBound"      ,str(mx))

    def getMinMax(self):
        filterExp = self.mainSQL['filter']
        if "where" in filterExp :
            filterExp += "  and {0} is not null".format(self.parms["--fetchPartitionCol"])
        else :
            filterExp += "  where {0} is not null".format(self.parms["--fetchPartitionCol"])

        SQL = """
            select  'min' as TYP , min({0}) as VAL
            {1}
            {2}
            union all
            select  'max' as TYP , max({0}) as VAL
            {1}
            {2}
            """.format(self.parms["--fetchPartitionCol"],self.mainSQL['from'],filterExp)

        self.log.info("Fetching min amd max value using the sql: \n\t%s" %  (SQL) )

        min_max = self.spark.read \
                        .format("jdbc") \
                        .option("fetchsize", "2")   \
                        .option("user"     , self.parms["--user"]) \
                        .option("password" , self.parms["--password"]) \
                        .option("driver"   , self.parms["--jdbcDriver"] ) \
                        .option("url"      , self.parms["--jdbcUrl"]) \
                        .option("dbtable"  , " ( {} ) x".format(SQL)) \
                        .load().collect()

        (mnVal , mxVal) =     (min_max[0]['VAL'] , min_max[1]['VAL'])  if min_max[0]['TYP'] == 'min' \
                        else (min_max[1]['VAL'] , min_max[0]['VAL'])

        self.log.info("min({0}): {1} , max({0}):{2}"\
                    .format(self.parms["--fetchPartitionCol"], mnVal, mxVal) )

        if mnVal == None or mxVal == None :
            self.log.error("Not able to find the min and max vales (got Null result)." )
            self.log.error("Returned Records are, \n\t{}".format(min_max))
            return  (None, None)
        return (int(mnVal), int(mxVal))

    def getMinOrMax(self,typ):
        filterExp = self.mainSQL['filter']
        if " where " in filterExp :
            filterExp += "  and {0} is not null".format(self.parms["--fetchPartitionCol"])
        else :
            filterExp += "  where {0} is not null".format(self.parms["--fetchPartitionCol"])

        SQL = """
        select {0}({1}) as VAL
        {2}
        {3}
        """.format(typ,self.parms["--fetchPartitionCol"], self.mainSQL['from'],filterExp)

        self.log.info("Fetching %s value using the sql: \n\t%s" %  (typ,SQL) )

        min_max = self.spark.read \
                        .format("jdbc") \
                        .option("fetchsize" , "2")   \
                        .option("user"      , self.parms["--user"]) \
                        .option("password"  , self.parms["--password"]) \
                        .option("driver"    , self.parms["--jdbcDriver"] ) \
                        .option("url"       , self.parms["--jdbcUrl"]) \
                        .option("dbtable"   ," ( {} ) x".format(SQL)) \
                        .load().collect()

        val =  min_max[0]['VAL']
        self.log.info("Derived {0}({1}): {2}"\
                    .format(typ,self.parms["--fetchPartitionCol"], val) )

        if val == None :
            self.log.error("Not able to find the {0} vales (got Null result).".format(typ) )
            self.log.error("Returned Records are, \n\t{}".format(min_max))
            return None
        else:
            return int(val)

    def reSetColumnOrder(self,jdbcDFFlds):
        idflds = list ( map(lambda c: c.strip().upper() ,
                            jdbcDFFlds if 'overwrite' == self.parms['--writeMode'] \
                            else self.spark.sql("select * from {0}.{1} where 1 <>1"
                                        .format(self.parms["--dstDB"],self.parms["--dstTable"])) \
                                    .schema.names
                    ))

        partCols = []
        if '--writePartitionCol' in self.parms:
            partCols = list (map(lambda c: c.strip().upper() , self.parms["--writePartitionCol"].split(",") ))

        fldList = list(filter(lambda f, p=partCols :  f not in p, idflds))
        for c in partCols :
            fldList.append(c)

        return ",".join(fldList)

    def createOrReplaceTable(self,xferSQL):
        self.spark.sql("drop table if exists {0}.{1}".format(self.parms["--dstDB"],self.parms["--dstTable"]))
        partitionBy = ''
        if "--writePartitionCol" in self.parms:
            partitionBy  = "partitioned by ( {} ) ".format(self.parms["--writePartitionCol"])
            ddlSql = """
            create table {0}.{1} using {2} {3}
            TBLPROPERTIES ("auto.purge"="true")
                as  {4}
            """.format(self.parms["--dstDB"],
                       self.parms["--dstTable"],
                       self.parms["--outFlFmt"],
                       partitionBy,
                       xferSQL)
            logln = "Creating the table {0}.{1} using the DDL,\n{2}" \
                    .format(self.parms["--dstDB"],self.parms["--dstTable"],ddlSql)
            self.log.info(logln)
            self.spark.sql(ddlSql)

    def insertIntoTable(self,df,xferSQL):
        tablePath = self.spark.sql("describe formatted {0}.{1}".format(self.parms['--dstDB'], self.parms['--dstTable'])) \
                        .filter("col_name = 'Location'")\
                        .collect()[0][1]
        insertType = "insert into"
        partition =   "partition ( {} )".format(self.parms["--writePartitionCol"]) \
                if "--writePartitionCol" in self.parms else " "

        if self.parms["--writeMode"] == 'dropPartFirst':
            self.dropPartitions(df, self.parms["--writePartitionCol"], tablePath, True)
        elif self.parms["--writeMode"] == 'insertOverwrite':
            insertType = "insert overwrite table "

        insertSQL = """
        {0} {1}.{2} {3} {4}
        """.format(insertType,
                    self.parms["--dstDB"],
                    self.parms["--dstTable"],
                    partition,
                    xferSQL)
        logLn = "Inserting records into the table {0}.{1} using the SQL,{2}"\
                .format(self.parms["--dstDB"],self.parms["--dstTable"],insertSQL)
        self.log.info(logLn)
        self.spark.sql(insertSQL)

    def dropPartitions(self,df, partitionColList, tablePath, deleteFiles=True):
        self.log.info("Deleting the partitions (if exists) under the s3 Path : {} ".format (tablePath))
        dfCols = self.getDistinctWritePartionColValues( df, partitionColList)

        for x in dfCols.collect():
            (bkt,key) = self.s3.getBucketNKeyTuple( "{0}/{1}".format(tablePath,x['PARTITIONS'].replace("'","")))
            if deleteFiles:
                self.s3.deleteS3Object (bkt,key)
            sql = "ALTER TABLE {0}.{1} DROP IF EXISTS PARTITION ({2})" \
                .format( self.parms["--dstDB"],self.parms["--dstTable"], x['PARTITIONS'])
            self.log.info("Altering the table by running the SQL,{}".format(sql))
            self.spark.sql(sql)
            self.spark.sql("Msck repair table {0}.{1}".format( self.parms["--dstDB"],self.parms["--dstTable"]))
            self.spark.sql("refresh table {0}.{1}".format( self.parms["--dstDB"],self.parms["--dstTable"]))

    def getDistinctWritePartionColValues(self,df,partitionColList):
        split  = list(map(lambda c: c.strip().upper(),  partitionColList.split(",")))
        fields = list(filter(lambda f: (f.name.upper() in split)  , df.schema.fields ))

        self.log.debug ("splits: {}".format(split))
        self.log.debug ("fields: {}".format(str(fields)))

        #https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-CreatePartition
        def getFltr(split):
            res = None
            for field in fields:
                self.log.debug("split: {} , filed {} of type {}".format(split, field, str(field.dataType)))
                if field.name.upper() == split.upper():
                    self.log.debug("field name {} match with {}".format(field.name, split))
                    if type(field.dataType) in [dfTypes.StringType,dfTypes.DateType,dfTypes.TimestampType]:
                        res = "'{0}=' , '{0}'".format(field.name.upper())
                        self.log.info("inside String type")
                    elif type(field.dataType) in [dfTypes.IntegerType, dfTypes.LongType, dfTypes.ShortType, dfTypes.DecimalType,dfTypes.DoubleType,dfTypes.FloatType]:
                        res = "'{0}=' , {0}".format(field.name.upper())
                        self.log.info("inside Numeric type")
                    else:
                        self.log.info("filed's type didn't match :( {}".format(type(field.dataType)))
                else :
                    self.log.debug("field name {} did not match with {}".format(field.name, split))
                if res != None:
                    break
            self.log.debug("Final Result  {}".format(res))
            return res

        pFields= list(map(getFltr, split))
        columns = "concat( "+ " , ',', ".join( pFields ) + " ) as PARTITIONS"

        self.log.debug ("pFields: {}".format(str(pFields)))
        self.log.debug("concat stmt: {0}".format(columns))

        return df.selectExpr(columns).distinct()

if __name__ == '__main__':
    Ingest().start(sys.argv)
