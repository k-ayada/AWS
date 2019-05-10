
#BootStrap- Starts
pip-3.6 install --user --upgrade boto3 pandas fastparquet s3fs google-cloud-bigquery pyarrow
export PATH=$PATH:~/.local/bin
mkdir ~/gaload
cd ~/gaload
aws s3 sync s3://cloudera-migration/kiran/pySpark/ .

#BootStrap - Ends

#Step CLI
python3 GoogleBigQuery.py  \
    --tempS3 "s3://prod-gwf-datalake-temp-us-east-1/ga_load/athena_res"  \
    --encryptionType SSE_S3  \
    --kmsKeyArn "arn:aws:kms:us-east-1:426625017959:key/f826ca73-d3fa-4f91-be18-0b97ebde2f29"  \
    --awsRole "arn:aws:iam::426625017959:role/Prod-BigDataETLProdSupp"
