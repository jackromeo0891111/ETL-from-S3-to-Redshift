# ETL-from-S3-to-Redshift
This project serves the purpose for an imaginary startup company "Sparkify" to fetch user data from AWS S3 and transfer to Amazon Redshift data warehouse for future analysis
### Database schema design and ETL process  
#### 1.Database schema    
##### Staging tables   
![Screen Shot 2022-12-18 at 16 53 10](https://user-images.githubusercontent.com/88352138/208328830-2044b52e-ef9a-4c6f-86f9-ab55adf2189c.png)

##### Final tables   
![Screen Shot 2022-12-18 at 16 39 08](https://user-images.githubusercontent.com/88352138/208328128-c160277c-fb4e-4970-a3cb-cf38379bcabc.png)

#### 2. ETL process    
1. Copy files from S3 and transform, then insert into staging tables
2. Copy from staging tables into each final tables 
![Screen Shot 2022-12-18 at 17 05 41](https://user-images.githubusercontent.com/88352138/208329553-1748538d-118d-47a1-9730-08f9cfaa99cd.png)

### How to run the python scripts
1. Edit dwh.cfg. Fill in Amazon Redshift info, IAM role and S3 file path
2. Run create_tables.py to create table schemas in Redshift
3. Run etl.py to transfer files in S3 and insert into staging tables/final tables
