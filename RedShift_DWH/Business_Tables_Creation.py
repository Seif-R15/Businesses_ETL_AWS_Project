import json
import boto3
import csv
import io
import psycopg2
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Generate timestamps
    start_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')  # For Redshift-compatible timestamp
    csv_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')  # For unique CSV filenames

    conn = None
    cursor = None

    try:
        # Extract bucket name and object key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        # Get the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')

        # Connecting to Redshift
        conn = psycopg2.connect(
            dbname='actual_products', 
            user='aws_user_name', 
            password='Redshift_Password', 
            host='redshift-cluster-1.ckgij7mqpm55.eu-north-1.redshift.amazonaws.com', 
            port='5439'
        )
        cursor = conn.cursor()

        # Creating staging table
        cursor.execute("DROP TABLE IF EXISTS staging;")
        cursor.execute("""
        CREATE TABLE staging (
            ProductID INT,
            ProductName VARCHAR(255),
            SupplierID INT,
            CategoryID INT,
            Unit VARCHAR(50),
            Price DECIMAL(10, 2)
        );
        """)

        # Read CSV content
        csv_reader = csv.reader(io.StringIO(file_content))
        next(csv_reader)  # Skip header

        # Insert rows into staging table
        insert_query = """
        INSERT INTO staging (ProductID, ProductName, SupplierID, CategoryID, Unit, Price)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for row in csv_reader:
            cursor.execute(insert_query, row)

        conn.commit()

        # Identify new rows and count them
        cursor.execute("""
        SELECT COUNT(*) FROM staging
        WHERE ProductID NOT IN (SELECT ProductID FROM product);
        """)
        insert_count = cursor.fetchone()[0]

        # Insert new rows into product table
        cursor.execute("""
        INSERT INTO product (ProductID, ProductName, SupplierID, CategoryID, Unit, Price)
        SELECT ProductID, ProductName, SupplierID, CategoryID, Unit, Price
        FROM staging
        WHERE ProductID NOT IN (SELECT ProductID FROM product);
        """)
        conn.commit()

        # Insert audit entry
        cursor.execute("""
        INSERT INTO audit (update_count, insert_count, start_date, end_date)
        VALUES (%s, %s, %s, %s)
        """, (0, insert_count, start_time, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

    finally:
        # Close connections in all cases
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps("Lambda function executed successfully.")
    }
