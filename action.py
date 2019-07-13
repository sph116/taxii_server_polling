from minemeld_taxii_client import get_data_main
from minemeld_taxii_client.mysql_model import Mysql_model
import schedule
from sever_query import views



def Action(id_data, url, input_time, table_name, bg_time, timing_type):

    """
    程序启动函数
    :param id_data:
    :param url:
    :param input_time:
    :param now_time:
    :param table_name:
    :param bg_time:
    :param timing_type:
    :return:
    """

    Mysql_model.create_ls(table_name)    #服务启动 创建服务相关表单   sever_name_date  sever_name_log
    if timing_type == "every_day":

        """每日轮询"""
        schedule.every().day.at(input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)  #设置定时器

    if timing_type == "every_hour":

        """每小时轮询"""
        schedule.every().hour.at(":" + input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)

    if timing_type == "every_week":

        """每周轮询"""
        week_day = int(input_time.split("-")[0])   #提取星期
        input_time = input_time.split("-")[1]      #提取 小时 与 分钟

        if week_day == 1:
            schedule.every().monday.at(input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)
        if week_day == 2:
            schedule.every().tuesday.at(input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)
        if week_day == 3:
            schedule.every().wednesday.at(input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)
        if week_day == 4:
            schedule.every().thursday.at(input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)
        if week_day == 5:
            schedule.every().friday.at(input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)
        if week_day == 6:
            schedule.every().saturday.at(input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)
        if week_day == 7:
            schedule.every().sunday.at(input_time).do(get_data_main.action_funtion, id_data, bg_time, url, input_time, table_name, timing_type)


    if timing_type == "one_time":
        """单次轮询"""
        get_data_main.action_funtion(id_data, bg_time, url, input_time, table_name, timing_type)

    while True:
        flag = views.get_flag(table_name)
        if timing_type == "one_time":  #单次请求 退出循环
            break
        if flag == 1:     #默认状态 或继续运行状态
            schedule.run_pending()  #运行程序

        if flag == 2:   #暂停程序  进行休眠
            print("暂停服务")
            while 1:
                flag = views.get_flag(table_name)
                if flag == 1:  #程序继续运行 退出循环
                    print("继续服务")
                    break
                if flag == 0:
                    break
                # time.sleep(1)
        if flag == 0:
            print("退出服务")
            break


