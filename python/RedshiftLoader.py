from __future__          import print_function
import sys,logging,boto3,psycopg2

from multiprocessing import Pool

class PyUtils:
    def __init__(self, **kwargs):
        self.log  = kwargs['log']

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

class AWS:
    def __init__(self, **kwargs):
        self.log  = kwargs['log']
        self.ssm = self.SSM(log=self.log, **kwargs)
        self.rs  = self.Redshift(log=self.log, **kwargs)

    def getNewRsConn(self,**kwargs):
        return self.Redshift(log=self.log,**kwargs)

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

    class Redshift:
        def __init__(self, **kwargs):
            self.log             = kwargs['log']
            self.__dbUsername    = kwargs['dbUsername']
            self.__dbPassword    = kwargs['dbPassword']
            self.__dbHost        = kwargs['dbHost']
            self.__dbPort        = kwargs['dbPort']
            self.__dbName        = kwargs['dbName']
            self.__rsClient      = boto3.client('redshift')
            self.__rsConn        = None
            self.__rsCurr        = None

        def __del__(self):
            self.close()

        def close(self):
            ''' Close the cursor and the connection '''
            if self.__rsCurr is not None:
                self.__rsCurr.close()
            if self.__rsConn is not None:
                self.__rsConn.close()

        def setdbUsername(self,dbUsername) : self.__dbUsername = dbUsername
        def setDBPassword(self,dbPassword) : self.__dbPassword = dbPassword
        def setDBHost    (self,dbHost )    : self.__dbHost     = dbHost
        def setDBPort    (self,dbPort )    : self.__dbPort     = dbPort
        def setDBName    (self,dbName )    : self.__dbName     = dbName

        def __parseJdbcConnStr(self,jdbcConnStr):
            '''
                Parse the JDBc connection string and retrieve the host,port adn DB names
                Expected format
                jdbc:redshift://<host>:<port>/<dbName>
            '''
            self.__dbHost, port_db       = jdbcConnStr.split("//")[1:][0].split(":")
            self.__dbPort, self.__dbName = port_db.split("/")

        def connect(self, jdbcConnStr=None, dbUsername=None, dbPassword=None):
            ''' Connect to the Redshift cluster  '''
            if self.__rsConn is None:
                if jdbcConnStr is not None : self.__parseJdbcConnStr(self,jdbcConnStr)
                if dbUsername  is not None : self.__dbUsername = dbUsername
                if dbPassword  is not None : self.__dbPassword = dbPassword
                try:
                    self.__rsConn = psycopg2.connect(
                                    dbname   = self.__dbName,
                                    host     = self.__dbHost,
                                    port     = self.__dbPort,
                                    user     = self.__dbUsername,
                                    password = self.__dbPassword)
                except (Exception, psycopg2.Error) as error :
                    self.log.error("Failed to connect to RS. Error : ", error)
                    raise
            else :
                print("Redshift Connectionis already initialized..")

        def select(self,sql):
            ''' execute a select query and return the results as list'''
            self.log.info("Running the SQL,\n{0}".format(sql))
            try:
                if self.__rsConn == None :
                    self.connect()
                if self.__rsCurr != None :
                    self.__rsCurr = self.__rsConn.cursor()

                self.__rsCurr.execute(sql)
            except (Exception, psycopg2.Error) as error :
                self.log.error("Error while fetching data RS, {0}".format(error))
                self.close()
                raise

            if self.__rsCurr:
                res = self.__rsCurr.fetchall()
            else:
                res = None

            self.close()
            return res

        def execute(self,sql):
            ''' Execute DDLs or DMLs  '''
            self.log.info("Running the SQL,\n{0}".format(sql))
            try:
                if self.__rsConn == None :
                    self.connect()
                if self.__rsCurr != None :
                    self.__rsCurr = self.__rsConn.cursor()

                self.__rsCurr.execute(sql)
                self.__rsConn.commit()
            except (Exception, psycopg2.Error) as error :
                self.log.error("Error executing the DDL/DML, {0}".format(error))
                self.close()
                raise
            self.close()

class RedshiftLoader:
    def __init__(self, **kwargs):
        self.log   = logging.getLogger(__name__)
        self.pyu   = PyUtils(log=self.log)
        self.parms = self.pyu.prseParms(kwargs['argv'])
        self.aws   = AWS(log=self.log)
        self.ssm   = self.aws.ssm
        self.rs    = self.aws.rs
        self.jdbcConnStr = None
        self.dbUsername  = None
        self.dbPassword  = None

    def runCOPY(self,prm):
        sql = """
        copy    {0}.{1}
        from   '{2}'
        format as {3}
        {4}
        """.format(prm['db'],prm['table'],prm['filePath'],prm['flFormat'], prm['copyOPts'])

        rs = self.aws.getNewRsConn(
            dbUsername = self.dbUsername,
            dbPassword = self.dbPassword,
            dbHost     = self.dbHost,
            dbPort     = self.dbPort,
            dbName     = self.dbName
        )
        self.log.info("{0} - starting the COPY comamnd\n {1}".format(prm['table'],sql))
        rs.connect()
        rs.execute(sql)
        rs.close()
        self.log.info("{0} - Finished the COPY comamnd".format(prm['table']))

    def start(self):
        env = self.parms.get('env','prod')

        if '--jdbcConnStr' in self.parms:
            if self.parms['--jdbcConnStr'].startswith("ssm:/"):
                self.jdbcConnStr = self.ssm.get(self.parms['--jdbcConnStr'])
            else:
                self.jdbcConnStr = self.parms['--jdbcConnStr']
        else:
            self.jdbcConnStr = self.ssm.get("/datalake/{0}/redshift/connection".format(env))

        if '--dbUsername' in self.parms:
            if self.parms['--dbUsername'].startswith("ssm:/"):
                self.dbUsername = self.ssm.get(self.parms['--dbUsername'])
            else:
                self.dbUsername = self.parms['--dbUsername']
        else:
            self.dbUsername = self.ssm.get("/datalake/{0}/redshift/username".format(env))

        if '--dbPassword' in self.parms:
            if self.parms['--dbPassword'].startswith("ssm:/"):
                self.dbPassword = self.ssm.get(self.parms['--dbPassword'])
            else:
                self.dbPassword = self.parms['--dbPassword']
        else:
            self.dbPassword = self.ssm.get("/datalake/{0}/redshift/password".format(env))

        poolArg= []
        for tbl in self.parms['--tables']:
            poolArg.append(
                   {'db'      : self.parms['--rsDB'],
                    'table'   : tbl,
                    'filePath': "{0}/{1}".format(self.parms['--filePath'],tbl),
                    'flFormat': self.parms['--flFormat'],
                    'copyOPts': self.parms['--copyOPts']
                   }
            )
        poolCount = int(self.parms.get("--threads","2"))
        self.log.info("Starting the processing with thread count of : {}" + poolCount)
        with Pool(processes= poolCount) as p:
            p.map(self.runCOPY,poolArg)

if __name__ == '__main__':
    RedshiftLoader(argv=sys.argv).start()
