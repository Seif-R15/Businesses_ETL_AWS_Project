import json
import boto3
import csv
import io
import psycopg2
from datetime import datetime

# Initialize S3 client
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
        
        # Connect to Redshift
        conn = psycopg2.connect(
            dbname='actual_products', 
            user='user', 
            password='Redshift_Password', 
            host='redshift-cluster-1.ckgij7mqpm55.eu-north-1.redshift.amazonaws.com', 
            port='5439'
        )
        cursor = conn.cursor()

        # Drop and recreate staging table
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
        next(csv_reader)  # Skip header row

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

        # Identify updated rows and store ProductIDs
        cursor.execute("""
        SELECT product.ProductID, product.ProductName, staging.ProductName, product.SupplierID, product.CategoryID, product.Unit, product.Price
        FROM product
        JOIN staging ON product.ProductID = staging.ProductID
        WHERE 
            product.ProductName != staging.ProductName OR
            product.SupplierID != staging.SupplierID OR
            product.CategoryID != staging.CategoryID OR
            product.Unit != staging.Unit OR
            product.Price != staging.Price;
        """)
        updated_products = cursor.fetchall()  # Store updated products for CSV generation

        # Perform the actual update
        cursor.execute("""
        UPDATE product
        SET 
            ProductName = staging.ProductName,
            SupplierID = staging.SupplierID,
            CategoryID = staging.CategoryID,
            Unit = staging.Unit,
            Price = staging.Price
        FROM staging
        WHERE product.ProductID = staging.ProductID
        AND (
            product.ProductName != staging.ProductName OR
            product.SupplierID != staging.SupplierID OR
            product.CategoryID != staging.CategoryID OR
            product.Unit != staging.Unit OR
            product.Price != staging.Price
        );
        """)

        conn.commit()

        # Record the end time of the function execution
        end_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        # Insert audit record with formatted start_time and end_time
        cursor.execute("""
        INSERT INTO audit (update_count, insert_count, start_date, end_date)
        VALUES (%s, %s, %s, %s);
        """, (len(updated_products), len(new_products), start_time, end_time))

        conn.commit()

        # Retrieve the most recent aud_id based on the start_date and end_date
        cursor.execute("""
        SELECT aud_id FROM audit
        WHERE start_date = %s AND end_date = %s;
        """, (start_time, end_time))
        aud_id = cursor.fetchone()[0]  # Fetch the aud_id of the most recent record

        # Save inserted rows as CSV files with csv_timestamp for uniqueness
        if new_products:
            csv_buffer = io.StringIO()
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerow(['ProductID', 'ProductName', 'SupplierID', 'CategoryID', 'Unit', 'Price'])  # Write header
            csv_writer.writerows(new_products)  # Write all columns of new products

            s3.put_object(
                Bucket=bucket_name,
                Key=f'inserts/insert_{csv_timestamp}.csv',
                Body=csv_buffer.getvalue()
            )

        # Save updated rows as CSV files with csv_timestamp for uniqueness
        if updated_products:
            csv_buffer = io.StringIO()
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerow(['ProductID', 'ProductName', 'SupplierID', 'CategoryID', 'Unit', 'Price'])  # Write header
            csv_writer.writerows(updated_products)

            s3.put_object(
                Bucket=bucket_name,
                Key=f'updates/update_{csv_timestamp}.csv',
                Body=csv_buffer.getvalue()
            )

        # Update the product table with the aud_id for newly inserted rows
        if new_products:
            cursor.execute("""
            UPDATE product
            SET aud_id = %s
            WHERE ProductID = ANY(%s);
            """, (aud_id, [row[0] for row in new_products]))  # Use list of new ProductIDs

        # Update the product table with the aud_id for updated rows
        if updated_products:
            cursor.execute("""
            UPDATE product
            SET aud_id = %s
            WHERE ProductID = ANY(%s);
            """, (aud_id, [row[0] for row in updated_products]))  # Use list of updated ProductIDs

        conn.commit()

        # Close the connection
        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps(f'CSV file processed. {len(new_products)} rows inserted, {len(updated_products)} rows updated. CSVs saved in S3.')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
