import threading
from queue import Queue
from minemeld_taxii_client.get_data_funtion import *
from minemeld_taxii_client.mysql_model import Mysql_model





def get_times(input_time, bg_time):
    times = (input_time - bg_time).total_seconds()    #计算定时时间 与 第一次向前轮询时间 的时间差 单位为秒数
    times = int(times)
    times = str(times // 3600) + "-" + str(times % 3600 // 60)  #计算向前轮询时间  构造格式为 “小时-分钟”
    return times



def action_funtion(id_data, bg_time, url, input_time, table_name, timing_type="one_time"):
    """
    程序运行主函数 包含线程调度 轮询时间修改
    :param id_data:
    :param bg_time:
    :param url:
    :param input_time:
    :param table_name:
    :param times:
    :param timing_type:
    :return:
    """


    threads = []  # 构造工作线程池
    now_time = datetime.now()  #获取轮询开始时候 的 当前时间

    poll_times = Mysql_model.get_poll_times(table_name)    #获取轮询次数
    if timing_type == "every_day":
        input_time = str(now_time.year) + "-" + str(now_time.month) + "-" + str(   #每日轮询条件下的字符串拼接
            now_time.day) + " " + input_time + ":00.000000"
        input_time = datetime.strptime(input_time, "%Y-%m-%d %H:%M:%S.%f")
        if poll_times == 0:                                                #第一次查询 根据起始时间进行查询
            print("每天轮询 第一次轮询")
            times = get_times(input_time, bg_time)
        else:
            print("每天轮询 后续轮询")
            times = "24"                                    #后续查询 进行12小时轮询

    if timing_type == "every_hour":
        input_time = str(now_time.year) + "-" + str(now_time.month) + "-" + str(
            now_time.day) + " " + str(now_time.hour) + ":" + input_time + ":00.000000"
        input_time = datetime.strptime(input_time, "%Y-%m-%d %H:%M:%S.%f")
        if poll_times == 0:
            print("每小时轮询 第一次轮询")
            times = get_times(input_time, bg_time)
        else:
            print("每小时轮询 后续轮询")
            times = "1"

    if timing_type == "every_week":
        input_time = str(now_time.year) + "-" + str(now_time.month) + "-" + str(
            now_time.day) + " " + input_time + ":00.000000"
        input_time = datetime.strptime(input_time, "%Y-%m-%d %H:%M:%S.%f")
        if poll_times == 0:
            print("每周轮询 第一次轮询")
            times = get_times(input_time, bg_time)
        else:
            print("每周轮询 后续轮询")
            times = "154"

    if timing_type == "one_time":
        print("启动单次轮询")

        input_time = str(now_time.year) + "-" + str(now_time.month) + "-" + str(
            now_time.day) + " " + str(now_time.hour) + ":" + str(now_time.minute) + ":00.000000"
        input_time = datetime.strptime(input_time, "%Y-%m-%d %H:%M:%S.%f")
        times = get_times(input_time, bg_time)

    Mysql_model.up_data_severs_data(table_name, poll_times)  # 每次启动轮询 修改状态字段

    class Spider(threading.Thread):
        """多线程类"""
        def __init__(self, Thread_name, func):
            super().__init__()
            self.Thread_name = Thread_name
            self.func = func

        def run(self):
            self.func(self.name)

    def worker(Thread_name):

        """
        采集线程工作方法
        :param Thread_name:
        :return:
        """
        print(Thread_name + ':启动')
        while not collection_names_queue.empty():   #队列不为空继续运行
            collection_name_data = collection_names_queue.get()
            poll_request_url(collection_name_data, id_data, times, Thread_name, input_time, table_name)


    def main():
        """
        主线程
        :return:
        """
        global collection_names_queue
        global go_on_flag

        start = time.time()
        threadNum = 5    # 线程数量

        f = threading.Thread(target=get_poll_collection_url, args=(id_data, url, 'Thread_get_url'))    #collection_name 采集线程 只运行一次
        f.start()
        time.sleep(2)

        thread_main_name = "thread_main:"
        print(thread_main_name, "启动")
        while True:
            i = 1
            go_on_flag = get_go_on_flag()   #循环获取 继续运行的标志位
            if go_on_flag == True:   #标志位为true 为获取到clooection_name

                collection_names_queue = Queue()  # 构造一个不限大小的队列
                collection_names_ls = get_collection_names()
                for i in range(len(collection_names_ls)):
                    collection_names_queue.put(collection_names_ls[i])

                """构造采集线程"""
                for i in range(1, threadNum + 1):
                    thread = Spider("Thread_" + str(i), worker)
                    thread.start()
                    threads.append(thread)

                """阻塞"""
                for thread in threads:
                    thread.join()

                end = time.time()
                print("-------------------------------")
                print("数据获取完成. 用时{}秒".format(end - start))
                break

            elif go_on_flag == "out_flag":    #标志位为out_flag 为失败次数2过多 退出程序
                print("thread_main:数据获取失败 退出程序")
                break
            else:             #其余 标志位 false  为cloeection_name获取失败 重新等待collection_name获取成功
                i = i + 1
                print("thread_main:collection_name未获取 等待获取成功")
                if i == 3:
                    break
                time.sleep(10)


        # print("数据获取完成. 采集次数".format())
        # print("数据获取完成. 采集条目")

    # if __name__ == "__main__":
    main()
    poll_times += 1  #每次运行成功  轮询次数加一
    retry_counts = get_retry_counts()


    if go_on_flag == "out_flag" or retry_counts > 5:
        Mysql_model.up_data_severs_data(table_name, poll_times, poll_statues="false") #轮询失败 更改轮询状态
        poll_statues = "incomplete"
    else:
        Mysql_model.up_data_severs_data(table_name, poll_times, poll_statues=True)   #每次轮询成功 更改轮询状态
        poll_statues = "complete"

    item = {
        "feeds_name": "user_AlienVault",
        "feeds_describe": "Data feed for user: AlienVault",
        "last_time": str(input_time),
        "statues": poll_statues
    }
    Mysql_model.up_data_sever_log(item, table_name)



