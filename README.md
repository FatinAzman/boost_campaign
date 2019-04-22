# boost_campaign
Here is the documentation for Boost 'Pre' and 'Post' Campaign analysis.

1.	The latitude longitude for the respective brand you can get it from this link
•	s3a://ada-dev/boost_brand_affinity/
2.	Run the geofence (refer to the previous email)
3.	Start using PySpark in Cloud9, the scripts are as follow;


# MCDONALDS

#geofence result 

data = "s3a://ada-dev/fatin/geofence/result/boost_mcd/boost_mcd/part-00000-07f9db77-2038-4607-9a38-d621a00bbf9f-c000.csv.gz"

geo = sqlContext.read.load(data, format = 'com.databricks.spark.csv', header='false',inferSchema='true') 

geo.count() #41564

```text
+-------+------------------------------------+--------+
|_c0    |_c1                                 |_c2     |
+-------+------------------------------------+--------+
|android|b784f414-319e-4f19-a119-d7a9172877c4|19000644|
|android|0992ba36-4c8a-45cc-b4ae-d682280c053c|17008470|
|android|7d2098a4-67dc-4707-b334-704a7be78da7|15000145|
|android|4f89466a-972f-4fa4-baa7-754488d5722c|14000365|
|android|41e3f8e9-a42c-4b12-8e6f-729716703136|13001694|
+-------+------------------------------------+--------+
```
geo = geo.drop('_c0','_c2')

geo1 = geo.withColumnRenamed('_c1','ifa')

```text
+------------------------------------+
|ifa                                 |
+------------------------------------+
|b784f414-319e-4f19-a119-d7a9172877c4|
|0992ba36-4c8a-45cc-b4ae-d682280c053c|
|7d2098a4-67dc-4707-b334-704a7be78da7|
|4f89466a-972f-4fa4-baa7-754488d5722c|
|41e3f8e9-a42c-4b12-8e6f-729716703136|
+------------------------------------+
```
#merge with persona table (march 2019)

data5 = "s3a://ada-dev/zankai/playstore-bundle/output-special-201903/Boost_persona_mar19/*.snappy.parquet"

asn_wide = sqlContext.read.parquet(data5)

asn_wide.count() #13180614 

asn_wide = asn_wide.select('ifa','Money-Managers')

```text
+------------------------------------+--------------+                           
|ifa                                 |Money-Managers|
+------------------------------------+--------------+
|64410391-a991-4bb4-8e8f-abc16c7d1355|0             |
|01a820fb-7145-49f0-ac32-4607729e8370|0             |
|18e138af-ea44-463a-975a-8c3edaf11bfb|0             |
|2317c6a9-bb20-410e-aef5-de5dd58b81b6|0             |
|261723cc-c211-4695-8884-ea3e9f0cc5cc|0             |
+------------------------------------+--------------+
```
join = geo1.join(asn_wide, geo1.ifa == asn_wide.ifa,how='inner').drop(geo1.ifa)

join.count() #41564

```text
+------------------------------------+--------------+                           
|ifa                                 |Money-Managers|
+------------------------------------+--------------+
|e0620016-0d4a-47f1-a3fa-09cb2c124767|0             |
|ea97fa94-5d34-4f91-8510-23f92099646b|0             |
|09dbcf81-b530-462d-a1f4-8e7271b4f9d6|0             |
|6026752d-4f8f-426d-96fa-a89a8c67ad52|0             |
|29878626-9d19-4e06-99f3-17559c2ce999|0             |
+------------------------------------+--------------+
```
mm = join.filter(join['Money-Managers'] > 0)

mm.count() #2151 


#merge with boost data (march 2019)

data_path = "s3a://ada-dev/fatin/boost/campaign/2019_04_17_cid_maid.csv"

boost = sqlContext.read.load(data_path, format = 'com.databricks.spark.csv', header='true',inferSchema='true')  

boost.count() #1134749

```text
+------------------------+------------------------------------+
|customerId              |MAID                                |
+------------------------+------------------------------------+
|5bc475fd1c043e000737788c|a32e020d-57e4-4c75-9c75-73821d5f772d|
|5a669f0dead80a0006098502|E3ADB5FE-299F-4771-BCF6-8532605F6FB8|
|5bdaed1450d8d20007223378|f109a7a2-7125-44de-8218-102e404a77f6|
|5c7054e2b375bb000789ffc0|6fed81b8-8eec-4b17-b8c8-dc9ccc968b62|
|5ba3a739c307ad0007f0c405|b0f64a55-4b51-4016-962d-8f1a44e3a0a1|
+------------------------+------------------------------------+
```
boost = boost.select(lower(col('MAID')).alias('ifa'))

boost1 = boost.withColumn('boost_user', lit(1))

```text
+------------------------------------+----------+
|ifa                                 |boost_user|
+------------------------------------+----------+
|a32e020d-57e4-4c75-9c75-73821d5f772d|1         |
|e3adb5fe-299f-4771-bcf6-8532605f6fb8|1         |
|f109a7a2-7125-44de-8218-102e404a77f6|1         |
|6fed81b8-8eec-4b17-b8c8-dc9ccc968b62|1         |
|b0f64a55-4b51-4016-962d-8f1a44e3a0a1|1         |
+------------------------------------+----------+
```
final = mm.join(boost1, mm.ifa==boost1.ifa,how='left').dropDuplicates().drop(boost1.ifa)

final.count() #2151

final1 = final.na.fill(0) 

```text
+-----------+------------------------------------+--------------+----------+    
|device_type|ifa                                 |Money_Managers|boost_user|
+-----------+------------------------------------+--------------+----------+
|android    |3790ac01-e1ae-4df3-9c99-82c27e847739|1             |1         |
|android    |93ee91ba-dce3-4deb-9e00-d6a456236be3|1             |0         |
|android    |50eec541-2f97-49f4-9246-a97cef8a5cce|1             |1         |
|android    |ff73bc70-f8b5-420f-96fe-ecb7dd85c724|1             |0         |
|android    |fcd560d9-7b9a-4070-87bf-9526cda199aa|1             |0         |
+-----------+------------------------------------+--------------+----------+
```
