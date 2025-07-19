import requests 
import boto3
import json 
import os 
from datetime import datetime
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import boto3
import io

#kaggle_usn=''
#kaggle_key=''
#kaggle_url='https://www.kaggle.com/datasets/willianoliveiragibin/grocery-inventory/data'
local_fine='inv_data.csv'
S3_bucket='myretaildatawarehouse-123'
#bronze_layer=''
aws_region='us-east-1'

num_records=1000000
num_products=100
num_stores=50
start_date=datetime(2024,1,1)
end_date=datetime(2025,1,1)

product_ids = [f"P{str(i).zfill(4)}" for i in range(1, num_products + 1)]
store_ids = [f"S{str(i).zfill(3)}" for i in range(1, num_stores + 1)]
categories = ["Electronics", "Groceries", "Clothing", "Stationery", "Furniture", "Toys"]

faker=Faker()

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

sales_data=[]
for _ in range(num_records):
    sales_data.append(
        [
            random.choice(store_ids),
            random.choice(product_ids),
            random.choice(categories),
            random_date(start_date,end_date).strftime("%Y-%m-%d"),
            random.randint(1, 20),
            round(random.uniform(5.0, 500.0), 2)


        ]
    )

df_sales = pd.DataFrame(sales_data, columns=["Store_ID", "Product_ID", "Category", "Date", "Quantity_Sold", "Price"])

# Reference Tables
product_master = pd.DataFrame({
    "Product_ID": product_ids,
    "Product_Name": [faker.word().capitalize() + " " + faker.word().capitalize() for _ in product_ids],
    "Category": [random.choice(categories) for _ in product_ids]
})

store_master = pd.DataFrame({
    "Store_ID": store_ids,
    "Region": [faker.city() for _ in store_ids],
    "Manager_Name": [faker.name() for _ in store_ids]
})    


s3 = boto3.client(
    service_name='s3',
    aws_access_key_id='your_key',
    aws_secret_access_key='your_secret_key',
    region_name='us-east-1'
)

def s3_upload(df,key):
    csv_buffer=io.StringIO()
    df.to_csv(csv_buffer,index=False)
    #s3=boto3.client('s3')
    s3.put_object(Bucket=S3_bucket,Key=key,Body=csv_buffer.getvalue())
    print(f"Uploaded: s3://{S3_bucket}/{key}")


s3_upload(df_sales, "retail-data/bronze/sales_data.csv")
s3_upload(product_master, "retail-data/reference/product_master.csv")
s3_upload(store_master, "retail-data/reference/store_master.csv")



