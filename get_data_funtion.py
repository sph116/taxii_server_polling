# -*- coding: utf-8 -*-
import datetime
import time
import pytz
from datetime import datetime, timedelta
import requests
import minemeld_taxii_client.ourstaxii.v11 as taxii11
import re
from minemeld_taxii_client.mysql_model import Mysql_model
import traceback

requests.packages.urllib3.disable_warnings()   #消除警告信息显示

go_on_flag = False           #是否获取到 collection_name的标志
all_collection_names = []    #存储collection_name
retry_counts = 0            #重试次数




head = {       #请求头
    'Content-Type': 'application/xml',
    'X-TAXII-Content-Type': 'urn:taxii.mitre.org:message:xml:1.1',
    'X-TAXII-Accept': 'urn:taxii.mitre.org:message:xml:1.1',
    'X-TAXII-Services': 'urn:taxii.mitre.org:services:1.1',
    'X-TAXII-Protocol': 'urn:taxii.mitre.org:protocol:https:1.0'
    }

def get_poll_collection_url(id_data, url, thread_name):
    """
    负责 获取collection_name
    :param id_data:
    :param times:
    :param url:
    :param thread_name:
    :return:
    """

    global all_collection_names
    global retry_counts
    global go_on_flag

    print("thread_get_collection_names: 启动。")

    while 1:
        time.sleep(1)
        response_message_discovery = requests.post(url, proxies=False, verify=False, headers=head,
                                                   data=taxii11.discovery_request(), auth=(id_data["user"], id_data["psw"]))
        try:
            if response_message_discovery.status_code == 200:

                collection_url = re.findall(r'Address>(.*?)</', response_message_discovery.text)[1]
                poll_url = re.findall(r'Address>(.*?)</', response_message_discovery.text)[2]
                taxii_user = id_data["user"]
                taxii_password = id_data["psw"]
                break
            else:
                retry_counts += 1
                print(thread_name + ":colloction失败 重新请求 " + str(response_message_discovery.status_code))
        except Exception as e:
            retry_counts += 1
            print(print(thread_name + ":discovery失败 重新请求, 原因", e))
        if retry_counts == 5:   #采集失败20次 改写 flag 退出程序
            go_on_flag = "out_flag"
            break

    retry_counts = 0
    while 1:
        time.sleep(1)
        try:
            response_message_collection = requests.post(collection_url, proxies=False, verify=False, headers=head, data=taxii11.collection_information_request(), auth=(taxii_user, taxii_password))
            if response_message_collection.status_code == 200:
                collection_name_ls = re.findall(r'collection_name="(.*?)"\s', response_message_collection.text)
                num = 0
                for i in collection_name_ls:
                    item = {
                        "poll_url": poll_url,
                        "collection_name": i,
                        "statues": "have_data"
                    }
                    all_collection_names.append(item)
                    num += 1
                print(thread_name + ":获取" + str(num) + "条目collection_name")
                go_on_flag = True
                break

            else:
                retry_counts += 1
                continue
        except Exception as e:
            retry_counts += 1
            print(thread_name + ":colloction失败 重新请求 ", e)

        if retry_counts == 5:    #采集失败5次 改写 flag 退出程序
            go_on_flag = "out_flag"
            break

    retry_counts = 0





def get_go_on_flag():
    """
    等待获取到collection_name 继续多线程请求的标志位
    :return:
    """
    global go_on_flag
    return go_on_flag

def get_retry_counts():
    """
    返回 重试次数 作为 程序运行完毕 是否运行成功的标准
    :return:
    """
    global retry_counts
    return retry_counts


def get_collection_names():
    """
    获取到collection_name
    :return:
    """
    global all_collection_names
    return all_collection_names


def poll_request_url(collection_name_data, id_data, times, Thread_name, input_time, table_name):
    """
    进入每个collection_name进行数据采集 数据存储进入mysql 失败信息存储进入mysql
    :param collectin_name_data:
    :param id_data:
    :param head:
    :param times:
    :param Thread_name:
    :param now_time:
    :return:
    """

    global retry_counts


    if "-" in times:  # 第一次轮询 向前查询时间 根据用户输入
        date_N_hours_ago = input_time - timedelta(hours=int(times.split("-")[0]), minutes=int(times.split("-")[1]))
    else:  # 后续轮询 向前查询时间 写死
        date_N_hours_ago = input_time - timedelta(hours=int(times))
    args_sts = date_N_hours_ago.strftime("%Y-%m-%d %H:%M:%S")
    args_ets = input_time.strftime("%Y-%m-%d %H:%M:%S")

    # tz = pytz.timezone('Asia/Shanghai')

    structTime = time.strptime(args_sts, '%Y-%m-%d %H:%M:%S')
    begin_ts = datetime(*structTime[:7])
    # begin_ts = begin_ts.replace(tzinfo=tz)
    begin_ts = begin_ts.replace(tzinfo=pytz.UTC)  # 开始时间

    eTime = time.strptime(args_ets, '%Y-%m-%d %H:%M:%S')
    end_ts = datetime(*eTime[:7])
    # end_ts = end_ts.replace(tzinfo=tz)
    end_ts = end_ts.replace(tzinfo=pytz.UTC)  # 结束时间

    taxii_user = id_data["user"]
    taxii_password = id_data["psw"]

    poll_url = collection_name_data["poll_url"]
    collectin_name = collection_name_data["collection_name"]
    print(begin_ts, end_ts)
    try:
        while 1:
            try:
                time.sleep(1)
                response_message_poll = requests.post(poll_url, proxies=False, verify=False, headers=head, data=taxii11.poll_request(collectin_name, begin_ts, end_ts), auth=(taxii_user, taxii_password))
                if response_message_poll.status_code == 200:
                    f = open("./minemeld_taxii_client/test/" + collectin_name + ".html", "w+")
                    f.write(response_message_poll.text)
                    # print(response_message_poll.text)
                    Indicator_timestamp_ls = re.findall(r'timestamp="(.*?)" xsi:type', response_message_poll.text)
                    break
                else:
                    retry_counts += 1
                    print(Thread_name + ":poll失败 重新请求 " + str(response_message_poll.status_code))

            except Exception as e:
                retry_counts += 1
                print(Thread_name + ":poll失败 重新请求 ", e)
            if retry_counts == 2:
                break

        if retry_counts == 2:
            print("Thread_name" + ":数据获取失败 退出程序")
        else:
            if Indicator_timestamp_ls == []:
                print(Thread_name + ":" + collectin_name + "无数据")

            else:
                Confidence_timestamp_ls = re.findall(r'Confidence timestamp="(.*?)">\s', response_message_poll.text)
                indicator_Title_ls = re.findall(r'<indicator:Title>(.*?)</indicator:Title>', response_message_poll.text)
                indicator_Type_ls = re.findall(r'indicator:Type xsi:type="(.*?)">', response_message_poll.text)
                indicator_Description_ls = re.findall(r'<indicator:Description>(.*?)<', response_message_poll.text)
                cybox_Title_ls = re.findall(r'<cybox:Title>(.*?)</cybox:Title>', response_message_poll.text)
                cybox_Properties_ls = re.findall(r'<cybox:Properties xsi:type="(.*?)"\s', response_message_poll.text)
                object_type_ls = re.findall(r'" category="(.*?)">|" type="(.*?)">', response_message_poll.text)
                object_type_ls = [i[1] if len(i[0]) == 0 else i[0] for i in object_type_ls]
                # if object_type_ls == []:
                #     object_type_ls = re.findall(r'" type="(.*?)">', response_message_poll.text)
                object_Value_ls = re.findall(
                    r'Address_Value>(.*?)</AddressObj:Address_Value>|<URIObj:Value>(.*?)</URIObj:Value>|<DomainNameObj:Value>(.*?)</DomainNameObj:Value>',
                    response_message_poll.text)
                object_Value_ls = [j for i in object_Value_ls for j in i if len(j) != 0]
                # if object_Value_ls == []:
                #     object_Value_ls = re.findall(r'<URIObj:Value>(.*?)</URIObj:Value>', response_message_poll.text)
                stixCommon_ls = re.findall(r'<stixCommon:Value>(.*?)</stixCommon:Value>', response_message_poll.text)
                save_data = [{
                        "collection_name": collectin_name,
                        "Indicator_timestamp": Indicator_timestamp_ls[i],
                        "Confidence_timestamp": Confidence_timestamp_ls[i],
                        "indicator_Title": indicator_Title_ls[i],
                        "indicator_Type": indicator_Type_ls[i],
                        "indicator_Description": indicator_Description_ls[i],
                        "cybox_Title": cybox_Title_ls[i],
                        "cybox_Properties": cybox_Properties_ls[i],
                        "object_type": object_type_ls[i],
                        "object_Value": object_Value_ls[i],
                        "stixCommon": stixCommon_ls[i],
                    }for i in range(len(Confidence_timestamp_ls))]
                Mysql_model.save_data(save_data, Thread_name, table_name)
    except Exception as e:
        print(Thread_name + ":失败 原因", e, traceback.format_exc(), poll_url)
        exception_data = {
            "collection_name": collectin_name,
            "bg_end_time": str(begin_ts) + str(end_ts),
            "exception": repr(e),
            "poll_url": poll_url,
        }
        Mysql_model.save_exception_data(exception_data, Thread_name)
        f = open("./minemeld_taxii_client/test/" + collectin_name + ".html", "w+")
        f.write(response_message_poll.text)




