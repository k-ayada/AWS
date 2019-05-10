from __future__ import print_function # Python 2/3 compatibility
import os,sys,logging,uuid,time, pandas,json,traceback
import boto3 as boto
from boto3.dynamodb.conditions import Key, Attr
from dateutil import tz
from datetime import date,datetime, timedelta

from google.cloud import bigquery as bq
from google.oauth2 import service_account
import google.api_core.exceptions as bqException

from awsUtils.s3     import S3
from awsUtils.sts    import STS
from awsUtils.ssm    import SSM
from awsUtils.athena import Athena
from pyUtils.pyUtils import PyUtils


class GoogleBigQuery:
    def __init__(self):
        self._pyu   = PyUtils(logLvl = "i")
        self._parms = self._pyu.getParms()
        self._log   = self._pyu.getLogger()
        logOnType   =  'InstanceProfile'
        if '-useSAML'  in self._parms :
            logOnType   = 'SAML'
            if not '--awsRole' in self._parms:
                raise Exception("For SAML authentication, we need AWS Role ARN to be added with --awsRole")
        elif '-useCreds' in self._parms :
             logOnType  =   'CREDS'
        self._boto = STS(roleArn  = self._parms.get('--awsRole',"") ,
                         authType = logOnType ).assume_role()
        self._s3    = S3(self._log, self._boto)

        if '--fromYYYMMDD' in self._parms :
            self._frmDt =   datetime.strptime(self._parms.get('--fromYYYMMDD'), '%Y%m%d' )
        else:
             dt = '2018-10-03' #self.getLastRunDate()
             self._frmDt = datetime.strptime(dt, '%Y-%m-%d' )  + timedelta(days=1)

        if '--toYYYMMDD' in self._parms :
            self._tillDt =   datetime.strptime(self._parms.get('--toYYYMMDD'), '%Y%m%d' )
        elif '--forDays' in self._parms :
            dys = int(self._parms['--forDays']) - 1
            if dys > 0:
                self._tillDt =  self._frmDt + timedelta(days=dys)
            else :
                self._tillDt =  self._frmDt
        else:
             self._tillDt = datetime.now()


    def getLastRunDate(self) -> str:
        athena = athena = Athena(self._boto,
                                 self._parms['--tempS3'],
                                 {"type"   : self._parms['--encryptionType'],
                                  "KmsKey" : self._parms['--kmsKeyArn']
                    })
        res = athena.query(sql_str = 'select max(date) as max_dt from google_analytics_parquet.ga_data',
                           retainHeader = False)
        dt = res[0][0]
        self._log.info("current max(date) - > {}".format(dt))
        return dt

    def run(self):
        start = time.time()
        self._log.info("Extracting the Data from Date {} . Till {}".format(self._frmDt,self._tillDt))
        ssm    = SSM(botoSession = self._boto)
        ssmKey = '/datalake/ga/incremental/bigqueryCredentials'
        creds  = ssm.get_parm(ssmKey)[ssmKey]['Value']
        cred = json.loads(creds)
        credentials = service_account.Credentials.from_service_account_info(cred)
        project_id = credentials._project_id
        self.__bqClient = bq.Client(project_id, credentials=credentials)
        self.run_nxt()
        took = time.time() - start
        dlta = str(timedelta(seconds=took))
        self._log.info("Time taken to complete the full processing - > {}".format(dlta))

    def run_nxt(self):
        if self._frmDt > self._tillDt:
            pass
        else:
            frmDay = self._frmDt
            while frmDay <= self._tillDt:
                self.__prcDtStr = frmDay.strftime('%Y%m%d')
                self._log.info("Extracting the data for the day %s" % (self.__prcDtStr) )
                sql = self.buildSQL()
                start = time.time()
                self.getFrmBQ(sql = sql)
                frmDay  = frmDay + timedelta(days=1)
                took = time.time() - start
                dlta = str(timedelta(seconds=took))
                self._log.info("Time taken to complete the chunk - > {}".format(dlta))

    def buildSQL(self):
        sqlTxt = '''
           SELECT IFNULL(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime+hits.time/1000 AS INT64)), 'America/Denver'), '\000') as hit_timestamp,
                  IFNULL(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime AS INT64)), 'America/Denver'), '\000') as hit_visitStartTime,
                  IFNULL(CONCAT(SUBSTR(date,0,4),'-',SUBSTR(date,5,2),'-',SUBSTR(date,7,2)), '\000') as date,
                  IFNULL(CAST(visitNumber as STRING), '\000') as visitNumber,
                  IFNULL(CAST(visitId AS STRING), '\000') as visitId,
                  IFNULL(CAST(fullVisitorId AS STRING), '\000') as fullVisitorId,
                  IFNULL(CAST(totals.hits AS STRING), '\000') as hits,
                  IFNULL(CAST(totals.pageviews AS STRING), '\000') as pageviews,
                  IFNULL(CAST(totals.timeOnSite AS STRING), '\000') as timeOnSite,
                  IFNULL(CAST(totals.bounces AS STRING), '\000') as bounces,
                  IFNULL(CAST(totals.newVisits AS STRING), '\000') as newVisits,
                  IFNULL(CAST(totals.visits AS STRING), '\\000') as visits,
                  IFNULL(trafficSource.referralPath, '\000') as referralPath,
                  IFNULL(trafficSource.campaign, '\000') as campaign,
                  IFNULL(trafficSource.source, '\000') as source,
                  IFNULL(trafficSource.medium, '\000') as medium,
                  IFNULL(trafficSource.keyword, '\000') as keyword,
                  IFNULL(trafficSource.adContent, '\000') as adContent,
                  IFNULL(device.browser, '\000') as browser,
                  IFNULL(device.browserVersion, '\000') as browserVersion,
                  IFNULL(device.browserSize, '\000') as browserSize,
                  IFNULL(device.operatingSystem, '\000') as operatingSystem,
                  IFNULL(device.operatingSystemVersion, '\000') as operatingSystemVersion,
                  IFNULL(CAST(device.isMobile AS STRING), '\000') as isMobile,
                  IFNULL(device.mobileDeviceBranding, '\000') as mobileDeviceBranding,
                  IFNULL(device.flashVersion, '\000') as flashVersion,
                  IFNULL(CAST(device.javaEnabled AS STRING), '\000') as javaEnabled,
                  IFNULL(device.language, '\000') as language,
                  IFNULL(device.screenColors, '\000') as screenColors,
                  IFNULL(device.screenResolution, '\000') as screenResolution,
                  IFNULL(device.deviceCategory, '\000') as deviceCategory,
                  IFNULL(geoNetwork.continent, '\000') as continent,
                  IFNULL(geoNetwork.subContinent, '\000') as subContinent,
                  IFNULL(geoNetwork.country, '\000') as country,
                  IFNULL(geoNetwork.region, '\000') as region,
                  IFNULL(geoNetwork.metro, '\000') as metro,
                  IFNULL(hits.type, '\000') as type,
                  IFNULL(CAST(hits.hitNumber AS STRING), '\000') as hitNumber,
                  IFNULL(hits.social.socialInteractionNetwork, '\000') as socialInteractionNetwork,
                  IFNULL(hits.social.socialInteractionAction, '\000') as socialInteractionAction,
                  IFNULL(CAST(hits.time/1000 AS STRING), '\000') as hit_time,
                  IFNULL(CAST(hits.hour AS STRING), '\000') as hour,
                  IFNULL(CAST(hits.minute AS STRING), '\000') as minute,
                  IFNULL(CAST(hits.isSecure AS STRING), '\000') as isSecure,
                  IFNULL(CAST(hits.isInteraction AS STRING), '\000') as isInteraction,
                  IFNULL(CAST(hits.isEntrance AS STRING), '\000') as isEntrance,
                  IFNULL(CAST(hits.isExit AS STRING), '\000') as isExit,
                  IFNULL(hits.referer, '\000') as referer,
                  IFNULL(hits.page.pagePath, '\000') as pagePath,
                  IFNULL(hits.page.hostname, '\000') as hostname,
                  IFNULL(hits.page.pageTitle, '\000') as pageTitle,
                  IFNULL(hits.page.searchKeyword, '\000') as searchKeyword,
                  IFNULL(hits.page.searchCategory, '\000') as searchCategory,
                  IFNULL(hits.eventInfo.eventCategory, '\000') as eventCategory,
                  IFNULL(hits.eventInfo.eventAction, '\000') as eventAction,
                  IFNULL(hits.eventInfo.eventLabel, '\000') as eventLabel,
                  IFNULL(CAST(hits.eventInfo.eventValue AS STRING), '\000') as eventValue,
                  IFNULL((SELECT MAX(IF(index=1, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as LIAT_event_source_cd1,
                  IFNULL((SELECT MAX(IF(index=2, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as LIAT_event_id_cd2,
                  IFNULL((SELECT MAX(IF(index=3, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Slider_id_cd3,
                  IFNULL((SELECT MAX(IF(index=4, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Individual_id_cd4,
                  IFNULL((SELECT MAX(IF(index=5, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Plan_id_cd5,
                  IFNULL((SELECT MAX(IF(index=6, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Terminated_status_cd6,
                  IFNULL((SELECT MAX(IF(index=7, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as LIAT_Slider_Changed_Element_cd7,
                  IFNULL((SELECT MAX(IF(index=8, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as LIAT_Page_Name_cd8,
                  IFNULL((SELECT MAX(IF(index=9, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Login_Status_cd9,
                  IFNULL((SELECT MAX(IF(index=10, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Debug_cd10,
                  IFNULL((SELECT MAX(IF(index=11, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Age_cd11,
                  IFNULL((SELECT MAX(IF(index=12, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Salary_Group_cd12,
                  IFNULL((SELECT MAX(IF(index=13, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Gender_cd13,
                  IFNULL((SELECT MAX(IF(index=14, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as TransactionId_cd14,
                  IFNULL((SELECT MAX(IF(index=15, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Transaction_User_Id_cd15,
                  IFNULL((SELECT MAX(IF(index=16, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Unique_Event_Id_cd16,
                  IFNULL((SELECT MAX(IF(index=17, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as not_set_dimension_cd17,
                  IFNULL((SELECT MAX(IF(index=18, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as ValueUnits_cd18,
                  IFNULL((SELECT MAX(IF(index=19, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Deferral_Type_Code_cd19,
                  IFNULL((SELECT MAX(IF(index=20, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Income_Term_cd20,
                  IFNULL((SELECT MAX(IF(index=21, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Plan_Name_cd21,
                  IFNULL((SELECT MAX(IF(index=22, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Company_Match_Rule_cd22,
                  IFNULL((SELECT MAX(IF(index=23, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Company_Match_Description_cd23,
                  IFNULL((SELECT MAX(IF(index=24, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Has_Company_Match_cd24,
                  IFNULL((SELECT MAX(IF(index=25, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as TimeStamp_cd25,
                  IFNULL((SELECT MAX(IF(index=26, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Enrollment_Type_cd26,
                  IFNULL((SELECT MAX(IF(index=27, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as test_Liat_Page_cd27,
                  IFNULL((SELECT MAX(IF(index=28, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_ageselection_cd28,
                  IFNULL((SELECT MAX(IF(index=29, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_salaryselection_cd29,
                  IFNULL((SELECT MAX(IF(index=30, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_genderselection_cd30,
                  IFNULL((SELECT MAX(IF(index=31, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_my_pct_goal_cd31,
                  IFNULL((SELECT MAX(IF(index=32, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_mycontribution_cd32,
                  IFNULL((SELECT MAX(IF(index=33, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_mybalance_cd33,
                  IFNULL((SELECT MAX(IF(index=34, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_peer_pctgoal_cd34,
                  IFNULL((SELECT MAX(IF(index=35, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_peer_contributionrate_cd35,
                  IFNULL((SELECT MAX(IF(index=36, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_peer_balance_cd36,
                  IFNULL((SELECT MAX(IF(index=37, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_top_pct_goal_cd37,
                  IFNULL((SELECT MAX(IF(index=38, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_top_contributionrate_cd38,
                  IFNULL((SELECT MAX(IF(index=39, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as hdic_top_balance_cd39,
                  IFNULL((SELECT MAX(IF(index=40, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Age_Range_cd40,
                  IFNULL((SELECT MAX(IF(index=41, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as blank_test_dnu_cd41,
                  IFNULL((SELECT MAX(IF(index=42, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Url_Message_Param_cd42,
                  IFNULL((SELECT MAX(IF(index=43, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Url_Domain_Param_cd43,
                  IFNULL((SELECT MAX(IF(index=44, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as Url_jobNum_Param_cd44,
                  IFNULL((SELECT MAX(IF(index=45, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as test_instPart_id_cd45,
                  IFNULL((SELECT MAX(IF(index=46, value, NULL)) FROM UNNEST(hits.customDimensions)), '\000') as User_Agent_String_cd46,
                  IFNULL((SELECT MAX(IF(index=47, value, NULL)) FROM UNNEST(hits.customDimensions)), '\\000') as URL_accu_Param_cd47,
                  IFNULL(CAST((SELECT MAX(IF(index=1, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Slider_value_cm1,
                  IFNULL(CAST((SELECT MAX(IF(index=2, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Slider_previous_value_cm2,
                  IFNULL(CAST((SELECT MAX(IF(index=3, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Slider_Users_cm3,
                  IFNULL(CAST((SELECT MAX(IF(index=4, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Slider_End_Value_cm4,
                  IFNULL(CAST((SELECT MAX(IF(index=5, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Slider_Start_Value_cm5,
                  IFNULL(CAST((SELECT MAX(IF(index=6, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as  Saved_Retirement_Age_Start_Value_cm6,
                  IFNULL(CAST((SELECT MAX(IF(index=7, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as  Saved_Retirement_Age_End_Value_cm7,
                  IFNULL(CAST((SELECT MAX(IF(index=8, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Saved_Deferral_Rate_Start_Value_cm8,
                  IFNULL(CAST((SELECT MAX(IF(index=9, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Saved_Deferral_Rate_End_Value_cm9,
                  IFNULL(CAST((SELECT MAX(IF(index=10, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Equity_Start_Value_cm10,
                  IFNULL(CAST((SELECT MAX(IF(index=11, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Equity_End_Value_cm11,
                  IFNULL(CAST((SELECT MAX(IF(index=12, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Bond_Start_Value_cm12,
                  IFNULL(CAST((SELECT MAX(IF(index=13, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Bond_End_Value_cm13,
                  IFNULL(CAST((SELECT MAX(IF(index=14, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Salary_cm14,
                  IFNULL(CAST((SELECT MAX(IF(index=15, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Deferral_Rate_Change_Pct_cm15,
                  IFNULL(CAST((SELECT MAX(IF(index=16, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as BNS_Recommended_Dollar_cm16,
                  IFNULL(CAST((SELECT MAX(IF(index=17, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as BNS_Recommended_Pct_cm17,
                  IFNULL(CAST((SELECT MAX(IF(index=18, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Participant_Age_cm18,
                  IFNULL(CAST((SELECT MAX(IF(index=19, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Estimated_Monthly_Income_cm19,
                  IFNULL(CAST((SELECT MAX(IF(index=20, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Percent_of_Goal_cm20,
                  IFNULL(CAST((SELECT MAX(IF(index=21, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Income_Goal_cm21,
                  IFNULL(CAST((SELECT MAX(IF(index=22, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Income_Gap_cm22,
                  IFNULL(CAST((SELECT MAX(IF(index=23, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as test_Avg_Deferral_Start_cm23,
                  IFNULL(CAST((SELECT MAX(IF(index=24, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as test_Avg_Deferral_End_cm24,
                  IFNULL(CAST((SELECT MAX(IF(index=25, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Goal_Base_Salary_New_cm25,
                  IFNULL(CAST((SELECT MAX(IF(index=26, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Goal_Percent_Of_Salary_New_cm26,
                  IFNULL(CAST((SELECT MAX(IF(index=27, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Goal_Base_Salary_Previous_cm27,
                  IFNULL(CAST((SELECT MAX(IF(index=28, value, NULL)) FROM UNNEST(hits.customMetrics)) AS STRING), '\000') as Goal_Percent_Of_Salary_Previous_cm28,
                  IFNULL(hits.sourcePropertyInfo.sourcePropertyDisplayName, '\000') as sourcePropertyDisplayName,
                  IFNULL(hits.sourcePropertyInfo.sourcePropertyTrackingId, '\000') as sourcePropertyTrackingId
                FROM `empower-gw-analytics.93114548.ga_sessions_<processDate>`, UNNEST(hits) AS hits
        '''
        return sqlTxt.replace('<processDate>',self.__prcDtStr )

    def getFrmBQ(self, sql:str):
        # Run a Standard SQL query using the environment's default project
        try:
            df = self.__bqClient.query(sql).to_dataframe()
            self._s3.pandas2Parquet(pandasDF       = df,
                                bucket         = "cloudera-migration",
                                folder         = "krnydc/temp",
                                file           = "ga_data",
                                partition_cols = ['date'],
                                row_group_size = 10000000
                                )
            self._log.debug("Parquet files written :)")
        except Exception as  e:
            self._log.error("Failed to find the Table empower-gw-analytics.93114548.ga_sessions_{}".format(self.__prcDtStr))
            self._log.error(str(e))
            print("-"*60)
            traceback.print_exc(file=sys.stdout)
            print("-"*60)
           #self._log.error("SQL being executed,\n%s\n" % (sql))

def main():
    GoogleBigQuery().run()

#Get list of Tables to be loaded

if __name__ == "__main__":
    main()
'''
python3 GoogleBigQuery.py \
 --tempS3 "s3://prod-gwf-datalake-temp-us-east-1/ga_load/athena_res" \
 --encryptionType SSE_KMS \
 --kmsKeyArn "arn:aws:kms:us-east-1:426625017959:key/f826ca73-d3fa-4f91-be18-0b97ebde2f29" \
 --awsRole "arn:aws:iam::426625017959:role/Prod-BigDataETLProdSupp"
 -useSAML

python GoogleBigQuery.py --tempS3 "s3://cloudera-migration/temp/ga_load/athena_res"  --encryptionType SSE_KMS  --kmsKeyArn "arn:aws:kms:us-east-1:968074321407:key/35899729-dee1-47bd-9417-66ea59413988"  --awsRole "arn:aws:iam::968074321407:role/Dev-BigDataPowerDeveloper" -useSAML

'''

#https://blog.ruanbekker.com/blog/2018/05/09/temporary-iam-credentials-from-ec2-instance-metadata-using-python/
