Pipeline Overview

Data Store: Keep all the files in our AWS S3 storage Bucket.

Data Loading: Load the CSV file from AWS S3 storage Bucket to PostgreSQL using Airbyte.

Data Transformation: Transform the loaded data using SQL queries in dbt.

Data Validation: Run queries to validate the data quality.

Data Visualization: Use Metabase to visualize the transformed data and made connection from postgres to metabase.

Step-by-Step Instructions

1. Load CSV Data from AWS S3 to PostgreSQL using Airbyte
1.1. Configure Airbyte Source (Amazon S3)
Log in to Airbyte and navigate to the Sources tab

Create a new source with the following details:

Source Type: Amazon S3
Bucket Name: Name of your S3 bucket containing the CSV file.
Path Pattern: Path to the CSV file (e.g., data/*.csv).
File Format: Specify the CSV format.
Test the connection to ensure Airbyte can access the S3 bucket.

1.2. Configure Airbyte Destination (PostgreSQL)
Navigate to the Destinations tab in Airbyte.

Create a new destination with the following details:

Destination Type: PostgreSQL
Host: PostgreSQL server host
Port: PostgreSQL port (usually 5432)
Database: Name of your PostgreSQL database
Username/Password: PostgreSQL credentials
Test the connection to verify the configuration.

1.3. Set Up the Airbyte Sync
Create a new connection in Airbyte:

Source: Select the S3 source configured earlier.
Destination: Select the PostgreSQL destination.
Sync Mode: Select Full Refresh | Overwrite 
Run the sync to load data from the CSV file in S3 to PostgreSQL.

2. Transform Data in DBT
Use SQL queries to transform the data in DBT. The provided SQL queries perform data transformations and validations:

2.1. Transform the Tables into One Table
Run the following query in  to transform the tables into one table having one row per message:

sql:-

WITH status_ranked AS (
    SELECT
        message_id,
        status,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY timestamp DESC) AS rn
    FROM public."Noora_Statuses"
),
status_pivot AS (
    SELECT
        message_id,
        MAX(CASE WHEN rn = 1 THEN status END) AS status_1,
        MAX(CASE WHEN rn = 1 THEN timestamp END) AS timestamp_status_1,
        MAX(CASE WHEN rn = 2 THEN status END) AS status_2,
        MAX(CASE WHEN rn = 2 THEN timestamp END) AS timestamp_status_2,
        -- Add more statuses as needed
    FROM status_ranked
    GROUP BY message_id
)
SELECT
    m.id,
    m.content,
    m.message_type,
    -- Include other message fields as needed
    sp.status_1,
    sp.timestamp_status_1,
    sp.status_2,
    sp.timestamp_status_2,
    -- Include other status columns as needed
FROM public."Noora_Messages" m
LEFT JOIN status_pivot sp ON m.id = sp.message_id;

2.2. Detect and Flag Duplicate Records
Run this query to detect and flag duplicate records based on identical content and similar inserted_at timestamps:

sql:-

SELECT
    *,
    CASE
        WHEN COUNT(*) OVER (PARTITION BY content, DATE_TRUNC('minute', inserted_at)) > 1 THEN 1
        ELSE 0
    END AS is_duplicate
FROM
    public."Noora_Messages";
	
2.3. Additional Data Validation Queries
Check for Messages without Statuses:

sql:-

SELECT COUNT(*)
FROM public."Noora_Messages" m
LEFT JOIN public."Noora_Statuses" s ON m.id = s.message_id
WHERE s.message_id IS NULL;
Check for Unusual Timestamps:

sql:-

SELECT COUNT(*)
FROM public."Noora_Messages"
WHERE inserted_at > updated_at;
Check for Statuses without Corresponding Messages:

sql:-

SELECT COUNT(*)
FROM public."Noora_Statuses" s
LEFT JOIN public."Noora_Messages" m ON s.message_id = m.id
WHERE m.id IS NULL;
Check for Duplicate Records in Messages Table:

sql:-

SELECT COUNT(*)
FROM (
    SELECT id, COUNT(*)
    FROM public."Noora_Messages"
    GROUP BY id
    HAVING COUNT(*) > 1
) AS duplicates;
3. Visualize Data in Metabase
Connect Metabase to PostgreSQL:

In Metabase, go to Admin Settings > Databases.
Add a new database with PostgreSQL credentials.
Create Visualizations:

Use the queries provided to create visualizations in Metabase.

Example visualizations include:

Line Chart: Number of total and active users over time.

KPI Card: Fraction of sent messages that are read and the average time between when an outbound message is sent and when it is read.

Pie Chart: Number of outbound messages by status in the last week.

4. Example Queries for Visualization in Metabase
4.1. Number of Total and Active Users Over Time
sql:-

WITH weekly_user_data AS (
    SELECT
        DATE_TRUNC('week', inserted_at) AS week,
        masked_from_addr,
        MAX(CASE WHEN direction = 'inbound' THEN 1 ELSE 0 END) AS is_active_user,
        MAX(CASE WHEN direction IN ('inbound', 'outbound') THEN 1 ELSE 0 END) AS is_total_user
    FROM
       public."Noora_Messages"
    WHERE
        inserted_at >= NOW() - INTERVAL '6 months'
    GROUP BY
        week,
        masked_from_addr
)
SELECT
    week,
    COUNT(DISTINCT CASE WHEN is_total_user = 1 THEN masked_from_addr END) AS total_users,
    COUNT(DISTINCT CASE WHEN is_active_user = 1 THEN masked_from_addr END) AS active_users
FROM
    weekly_user_data
GROUP BY
    week
ORDER BY
    week;
	
4.2. Fraction of Sent Messages That Are Read
sql

WITH message_status AS (
    SELECT
        m.id,
        MIN(CASE WHEN s.status = 'sent' THEN s.timestamp END) AS sent_time,
        MIN(CASE WHEN s.status = 'read' THEN s.timestamp END) AS read_time
    FROM
        public."Noora_Messages" m
    LEFT JOIN
        public."Noora_Statuses" s ON m.id = s.message_id
    WHERE
        m.direction = 'outbound'
    GROUP BY
        m.id
)
SELECT
    COUNT(*) AS total_sent_messages,
    COUNT(read_time) AS total_read_messages,
    COUNT(read_time) * 1.0 / COUNT(*) AS read_fraction,
    AVG(EXTRACT(EPOCH FROM (read_time - sent_time)) / 60) AS avg_time_to_read_in_minutes
FROM
    message_status
WHERE
    sent_time IS NOT NULL;
4.3. Number of Outbound Messages by Status in the Last Week
sql:

SELECT
    s.status,
    COUNT(*) AS message_count
FROM
    public."Noora_Messages" m
JOIN 
    public."Noora_Statuses" s ON m.id = s.message_id
WHERE
    m.direction = 'outbound'
    AND m.inserted_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY
    s.status
ORDER BY
    message_count DESC;
	
5. Summary
This pipeline demonstrates how to extract data from Amazon S3, load it into a PostgreSQL database using Airbyte, transform the data using SQL queries through DBT, validate it, and finally visualize it in Metabase. Follow the steps in this guide to run the pipeline end-to-end.