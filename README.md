# taxii_server_polling
# taxii服务器的轮询采集及解析入库

## 功能：
1.使用schedule库实现每日，每天，每小时，每周对taxii_sever定时轮询。以及轮询的暂停，退出，继续机制。

2.多个collection_name将构造队列，采用多线程采集。

3.已实现退出机制，到达时间进入开始采集后，请求失败次数达到阈值将退出本次采集。

4.使用正则解析入库 提取多种ioc数据（ip,url,domain）使用mysql

5.根据时间戳实现增量式采集

6.为避免出现新的页面规则，解析失败的html文件及报错信息将进行存储，方便维护。


## 环境
语言:py3.6
库：schedule,pymysql,requests,traceback,pytz,configparser

## 启动
启动函数为./action.py.Action 启动前需修改ini文件mysql数据库信息
### 需要输入参数为
1.id_data 以字典形式存储taxii_client信息

2.url taxii_sever 地址

3.input_time 轮询时间

4.table_name 轮询名

5.bg_time 轮询采集的第一次采集向前查询时间

6.timing_type 轮询方式 every_week/every_day/every_hour/one_time

## 注：
1. 全部项目采用后端调度采集，后端调度未开源，单独使用需要自行修改。
