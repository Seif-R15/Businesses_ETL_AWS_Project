{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyMhPIHeAKgDyqKWQvmWH981"
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "dZHf2ztqyPmg"
      },
      "outputs": [],
      "source": [
        "import json\n",
        "import boto3\n",
        "import csv\n",
        "import io\n",
        "import psycopg2\n",
        "from datetime import datetime\n",
        "\n",
        "# Initialize S3 client\n",
        "s3 = boto3.client('s3')\n",
        "\n",
        "def lambda_handler(event, context):\n",
        "    # Generate timestamps\n",
        "    start_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')  # For Redshift-compatible timestamp\n",
        "    csv_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')  # For unique CSV filenames\n",
        "\n",
        "    try:\n",
        "        # Extract bucket name and object key from the event\n",
        "        bucket_name = event['Records'][0]['s3']['bucket']['name']\n",
        "        object_key = event['Records'][0]['s3']['object']['key']\n",
        "\n",
        "        # Get the file from S3\n",
        "        response = s3.get_object(Bucket=bucket_name, Key=object_key)\n",
        "        file_content = response['Body'].read().decode('utf-8')\n",
        "\n",
        "        # Connect to Redshift\n",
        "        conn = psycopg2.connect(\n",
        "            dbname='actual_products',\n",
        "            user='user',\n",
        "            password='Redshift_Password',\n",
        "            host='redshift-cluster-1.ckgij7mqpm55.eu-north-1.redshift.amazonaws.com',\n",
        "            port='5439'\n",
        "        )\n",
        "        cursor = conn.cursor()\n",
        "\n",
        "        # Drop and recreate staging table\n",
        "        cursor.execute(\"DROP TABLE IF EXISTS staging;\")\n",
        "        cursor.execute(\"\"\"\n",
        "        CREATE TABLE staging (\n",
        "            ProductID INT,\n",
        "            ProductName VARCHAR(255),\n",
        "            SupplierID INT,\n",
        "            CategoryID INT,\n",
        "            Unit VARCHAR(50),\n",
        "            Price DECIMAL(10, 2)\n",
        "        );\n",
        "        \"\"\")\n",
        "\n",
        "        # Read CSV content\n",
        "        csv_reader = csv.reader(io.StringIO(file_content))\n",
        "        next(csv_reader)  # Skip header row\n",
        "\n",
        "        # Insert rows into staging table\n",
        "        insert_query = \"\"\"\n",
        "        INSERT INTO staging (ProductID, ProductName, SupplierID, CategoryID, Unit, Price)\n",
        "        VALUES (%s, %s, %s, %s, %s, %s)\n",
        "        \"\"\"\n",
        "        for row in csv_reader:\n",
        "            cursor.execute(insert_query, row)\n",
        "\n",
        "        conn.commit()\n",
        "\n",
        "        # Identify newly inserted rows (fetch all relevant columns) and store the results\n",
        "        cursor.execute(\"\"\"\n",
        "        SELECT ProductID, ProductName, SupplierID, CategoryID, Unit, Price\n",
        "        FROM staging\n",
        "        WHERE ProductID NOT IN (SELECT ProductID FROM product);\n",
        "        \"\"\")\n",
        "        new_products = cursor.fetchall()  # Store new products for CSV generation\n",
        "\n",
        "        # Insert new rows into the product table\n",
        "        cursor.execute(\"\"\"\n",
        "        INSERT INTO product (ProductID, ProductName, SupplierID, CategoryID, Unit, Price)\n",
        "        SELECT ProductID, ProductName, SupplierID, CategoryID, Unit, Price\n",
        "        FROM staging\n",
        "        WHERE ProductID NOT IN (SELECT ProductID FROM product);\n",
        "        \"\"\")\n",
        "\n",
        "        conn.commit()\n",
        "\n",
        "        # Identify updated rows and store ProductIDs\n",
        "        cursor.execute(\"\"\"\n",
        "        SELECT product.ProductID, product.ProductName, staging.ProductName, product.SupplierID, product.CategoryID, product.Unit, product.Price\n",
        "        FROM product\n",
        "        JOIN staging ON product.ProductID = staging.ProductID\n",
        "        WHERE\n",
        "            product.ProductName != staging.ProductName OR\n",
        "            product.SupplierID != staging.SupplierID OR\n",
        "            product.CategoryID != staging.CategoryID OR\n",
        "            product.Unit != staging.Unit OR\n",
        "            product.Price != staging.Price;\n",
        "        \"\"\")\n",
        "        updated_products = cursor.fetchall()  # Store updated products for CSV generation\n",
        "\n",
        "        # Perform the actual update\n",
        "        cursor.execute(\"\"\"\n",
        "        UPDATE product\n",
        "        SET\n",
        "            ProductName = staging.ProductName,\n",
        "            SupplierID = staging.SupplierID,\n",
        "            CategoryID = staging.CategoryID,\n",
        "            Unit = staging.Unit,\n",
        "            Price = staging.Price\n",
        "        FROM staging\n",
        "        WHERE product.ProductID = staging.ProductID\n",
        "        AND (\n",
        "            product.ProductName != staging.ProductName OR\n",
        "            product.SupplierID != staging.SupplierID OR\n",
        "            product.CategoryID != staging.CategoryID OR\n",
        "            product.Unit != staging.Unit OR\n",
        "            product.Price != staging.Price\n",
        "        );\n",
        "        \"\"\")\n",
        "\n",
        "        conn.commit()\n",
        "\n",
        "        # Record the end time of the function execution\n",
        "        end_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')\n",
        "\n",
        "        # Insert audit record with formatted start_time and end_time\n",
        "        cursor.execute(\"\"\"\n",
        "        INSERT INTO audit (update_count, insert_count, start_date, end_date)\n",
        "        VALUES (%s, %s, %s, %s);\n",
        "        \"\"\", (len(updated_products), len(new_products), start_time, end_time))\n",
        "\n",
        "        conn.commit()\n",
        "\n",
        "        # Retrieve the most recent aud_id based on the start_date and end_date\n",
        "        cursor.execute(\"\"\"\n",
        "        SELECT aud_id FROM audit\n",
        "        WHERE start_date = %s AND end_date = %s;\n",
        "        \"\"\", (start_time, end_time))\n",
        "        aud_id = cursor.fetchone()[0]  # Fetch the aud_id of the most recent record\n",
        "\n",
        "        # Save inserted rows as CSV files with csv_timestamp for uniqueness\n",
        "        if new_products:\n",
        "            csv_buffer = io.StringIO()\n",
        "            csv_writer = csv.writer(csv_buffer)\n",
        "            csv_writer.writerow(['ProductID', 'ProductName', 'SupplierID', 'CategoryID', 'Unit', 'Price'])  # Write header\n",
        "            csv_writer.writerows(new_products)  # Write all columns of new products\n",
        "\n",
        "            s3.put_object(\n",
        "                Bucket=bucket_name,\n",
        "                Key=f'inserts/insert_{csv_timestamp}.csv',\n",
        "                Body=csv_buffer.getvalue()\n",
        "            )\n",
        "\n",
        "        # Save updated rows as CSV files with csv_timestamp for uniqueness\n",
        "        if updated_products:\n",
        "            csv_buffer = io.StringIO()\n",
        "            csv_writer = csv.writer(csv_buffer)\n",
        "            csv_writer.writerow(['ProductID', 'ProductName', 'SupplierID', 'CategoryID', 'Unit', 'Price'])  # Write header\n",
        "            csv_writer.writerows(updated_products)\n",
        "\n",
        "            s3.put_object(\n",
        "                Bucket=bucket_name,\n",
        "                Key=f'updates/update_{csv_timestamp}.csv',\n",
        "                Body=csv_buffer.getvalue()\n",
        "            )\n",
        "\n",
        "        # Update the product table with the aud_id for newly inserted rows\n",
        "        if new_products:\n",
        "            cursor.execute(\"\"\"\n",
        "            UPDATE product\n",
        "            SET aud_id = %s\n",
        "            WHERE ProductID = ANY(%s);\n",
        "            \"\"\", (aud_id, [row[0] for row in new_products]))  # Use list of new ProductIDs\n",
        "\n",
        "        # Update the product table with the aud_id for updated rows\n",
        "        if updated_products:\n",
        "            cursor.execute(\"\"\"\n",
        "            UPDATE product\n",
        "            SET aud_id = %s\n",
        "            WHERE ProductID = ANY(%s);\n",
        "            \"\"\", (aud_id, [row[0] for row in updated_products]))  # Use list of updated ProductIDs\n",
        "\n",
        "        conn.commit()\n",
        "\n",
        "        # Close the connection\n",
        "        cursor.close()\n",
        "        conn.close()\n",
        "\n",
        "        return {\n",
        "            'statusCode': 200,\n",
        "            'body': json.dumps(f'CSV file processed. {len(new_products)} rows inserted, {len(updated_products)} rows updated. CSVs saved in S3.')\n",
        "        }\n",
        "\n",
        "    except Exception as e:\n",
        "        return {\n",
        "            'statusCode': 500,\n",
        "            'body': json.dumps(f\"Error: {str(e)}\")\n",
        "        }\n"
      ]
    }
  ]
}