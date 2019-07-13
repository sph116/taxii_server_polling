import pymysql
import configparser
# from minemeld_taxii_client.action import get_sleep_time

class Operation_MySQL():

    def __init__(self):

        cf = configparser.ConfigParser()
        cf.read("./minemeld_taxii_client/config.ini", encoding="utf-8")
        self.host = cf.get("db", "host")
        self.user = cf.get("db", "user")
        self.password = cf.get("db", "password")
        self.port = int(cf.get("db", "port"))
        self.db = cf.get("db", "db")



    def create_ls(self, table_name):
        """建立相关数据库及数据表"""
        db = pymysql.connect(host=self.host, user=self.user,
                              passwd=self.password, charset='utf8')
        cur = db.cursor()
        # 开始建库
        # try:
        #     cur.execute("create database taxii character set utf8;")
        # except:
        #     pass
        # 使用库
        cur.execute("use zkbh_index;")
        # 建表
        try:

            cur.execute(
                "create table exception_data(collection_name VARCHAR (20),bg_end_time VARCHAR (299),exception VARCHAR (299))")
        except:
            pass
        try:
            cur.execute(
                "create table " + table_name + "_data(id int NOT NULL auto_increment primary key,collection_name VARCHAR (299),indicator_timestamp VARCHAR (299),Confidence_timestamp VARCHAR (299),indicator_Title VARCHAR (299),indicator_Type VARCHAR (299),indicator_Description VARCHAR (299),cybox_Title VARCHAR (500),cybox_Properties VARCHAR (299),object_type VARCHAR (299),object_Value VARCHAR (1000),stixCommon VARCHAR (299))")
            cur.execute(
                "create table " + table_name + "_log(feeds_name VARCHAR (299),feeds_describe VARCHAR (299),last_time VARCHAR (299),statues VARCHAR (299))")
        except Exception:
            print(Exception)
            pass
        db.close()




    def save_data(self, save_data, thread_name, table_name):
        """
        插入服务器轮询数据
        :param save_data:
        :param thread_name:
        :return:
        """
        db = pymysql.connect(host=self.host, user=self.user,
                             password=self.password, db=self.db, port=self.port)
        cur = db.cursor()

        for item in save_data:

            table = table_name + "_data"
            keys = ', '.join(item.keys())
            values = ', '.join(['%s'] * len(item))
            sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys,
                                                                         values=values)
            try:
                if cur.execute(sql, tuple(item.values())):
                    db.commit()

            except Exception as a:
                print(thread_name + ':插入数据失败, 原因', a, save_data[0]["collection_name"])
                exception_data = {
                    "collection_name": save_data[0]["collection_name"],
                    "bg_end_time": "no",
                    "exception": repr(a)
                }
                self.save_exception_data(exception_data, thread_name)
                db.rollback()
        print(thread_name + ":成功插入" + str(len(save_data)) + "条数据  collection_name=" + save_data[0]["collection_name"])
        db.close()


    def save_exception_data(self, item, thread_name):
        """
        存储错误数据
        :param item:
        :param thread_name:
        :return:
        """
        db = pymysql.connect(host=self.host, user=self.user,
                             password=self.password, db=self.db, port=self.port)
        cur = db.cursor()
        table = "exception_data"
        keys = ', '.join(item.keys())
        values = ', '.join(['%s'] * len(item))
        sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys,
                                                                     values=values)
        try:
            if cur.execute(sql, tuple(item.values())):
                db.commit()
        except Exception as a:
            db.rollback()
        print(thread_name + ":错误日志更新")
        db.close()

    def up_data_sever_log(self, item, table_name):
        """
        更新 服务器访问日志表信息
        :param item:
        :return:
        """
        db = pymysql.connect(host=self.host, user=self.user,
                             password=self.password, db=self.db, port=self.port)
        cur = db.cursor()
        table = table_name + "_log"
        keys = ', '.join(item.keys())
        values = ', '.join(['%s'] * len(item))
        sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys,
                                                                     values=values)
        try:
            if cur.execute(sql, tuple(item.values())):
                db.commit()

        except Exception as a:
            db.rollback()
        db.close()


    def up_data_severs_data(self, table_name, poll_times, poll_statues=False):
        """
        更新 severs_data表  轮询次数 及 轮询状态字段
        all work and no play make jack a dull boy  all work and no play make jack a dull boy
        :param item:
        :return:
        """
        db = pymysql.connect(host=self.host, user=self.user, password=self.password, port=self.port, db=self.db)
        cur = db.cursor()
        if poll_statues == True:
             cur.execute("update severs_date set poll_statues='complete' WHERE sever_name = %s", table_name)             #单次轮询结束 修改 statues字段为完成 修改轮询次数字段
             cur.execute("update severs_date set poll_times=poll_times+1 WHERE sever_name = %s", table_name)   #成功 轮数加一
        if poll_statues == "false":
            cur.execute("update severs_date set poll_times=poll_times+1 WHERE sever_name = %s", table_name)     #轮询失败 轮数加一
        else:
            cur.execute("update severs_date set poll_statues='incomplete' WHERE sever_name = %s", table_name)   #单次轮询开始 修改statues字段为未完成

        db.commit()   #提交
        db.close()

    def query_sever(self, table_name):
        """
        返回全部服务信息
        :param table_name:
        :return:
        """
        db = pymysql.connect(host=self.host, user=self.user, password=self.password, port=self.port, db=self.db)
        cur = db.cursor()
        table_name = table_name + "_log"
        cur.execute("select * from " + table_name)
        results = cur.fetchall()
        item = [{
            "feeds_name": row[0],
            "feeds_describe": row[1],
            "last_time": row[2],
            "statues": row[3]
        }for row in results]
        db.close()
        return item

    def get_poll_times(self, sever_name):
        """
        查询服务的轮询次数
        :param sever_name:
        :return:
        """
        db = pymysql.connect(host=self.host, user=self.user, password=self.password, port=self.port, db=self.db)
        cur = db.cursor()
        cur.execute("select poll_times from severs_date WHERE sever_name = %s", "taxii1")
        poll_times = cur.fetchone()[0]
        db.close()
        return poll_times







Mysql_model = Operation_MySQL()