import pymysql

# 建库和建表
con = pymysql.connect(host='localhost', user='root',
                      passwd='123456', charset='utf8')
cur = con.cursor()
# 开始建库
cur.execute("create database taxii character set utf8;")
# 使用库
cur.execute("use taxii;")
# 建表

cur.execute("create table exception_data(collection_name VARCHAR (20),bg_end_time VARCHAR (299),exception VARCHAR (299))")
cur.execute("create table taxii_data(collection_name VARCHAR (299),indicator_timestamp VARCHAR (299),Confidence_timestamp VARCHAR (299),indicator_Title VARCHAR (299),indicator_Type VARCHAR (299),indicator_Description VARCHAR (299),cybox_Title VARCHAR (299),cybox_Properties VARCHAR (299),object_type VARCHAR (299),object_Value VARCHAR (299),stixCommon VARCHAR (299))")
# cur.execute("create table zx_search_exception(url VARCHAR(100),exception VARCHAR(200))")