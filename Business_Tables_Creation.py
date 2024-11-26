import json
import boto3
import csv
import io
import psycopg2

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Generate timestamps
    start_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')  # For Redshift-compatible timestamp
    csv_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')  # For unique CSV filenames
    
    try:
        # Extract bucket name and object key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        # Get the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')

        # Connecting To Redshift DataBase
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
        next(csv_reader) 

        # Insert rows into staging table
        insert_query = """
        INSERT INTO staging (ProductID, ProductName, SupplierID, CategoryID, Unit, Price)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for row in csv_reader:
            cursor.execute(insert_query, row)
        
        conn.commit()

        # Identify newly inserted rows (fetch all relevant columns) and store the results
        cursor.execute("""
        SELECT ProductID, ProductName, SupplierID, CategoryID, Unit, Price
        FROM staging
        WHERE ProductID NOT IN (SELECT ProductID FROM product);
        """)
        new_products = cursor.fetchall()  # Store new products for CSV generation

        # Insert new rows into the product table
        cursor.execute("""
        INSERT INTO product (ProductID, ProductName, SupplierID, CategoryID, Unit, Price)
        SELECT ProductID, ProductName, SupplierID, CategoryID, Unit, Price
        FROM staging
        WHERE ProductID NOT IN (SELECT ProductID FROM product);
        """)

        conn.commit()

      # Creating other Main tables

       # Creating Audit Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit (
          Aud_id INT IDENTITY(1,1) PRIMARY KEY,  -- Auto-incremented ID for each Lambda execution
          update_count INT,                      -- Number of rows updated
          insert_count INT,                      -- Number of rows inserted
          ProductID INT,                         -- ProductID being inserted/updated
          start_date TIMESTAMP,                  -- Time when Lambda started
          end_date TIMESTAMP                     -- Time when Lambda finished
        );
        """)
        conn.commit()
      
        # Creating Product Table
        cursor.execute("""
        CREATE TABLE product (
          ProductID INTEGER NOT NULL,
          ProductName VARCHAR(255),
          SupplierID INTEGER,
          CategoryID INTEGER,
          Unit VARCHAR(255),
          Price DOUBLE PRECISION
        );
        """)
      
        # Close the connection
        cursor.close()
        conn.close()

      except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

