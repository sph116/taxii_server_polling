
import requests
import minemeld_taxii_client.ourstaxii.v11 as taxii11


def Sever_test(user_data, url):
    taxii_user = user_data["user"]
    taxii_password = user_data["psw"]

    head = {  # 请求头
        'Content-Type': 'application/xml',
        'X-TAXII-Content-Type': 'urn:taxii.mitre.org:message:xml:1.1',
        'X-TAXII-Accept': 'urn:taxii.mitre.org:message:xml:1.1',
        'X-TAXII-Services': 'urn:taxii.mitre.org:services:1.1',
        'X-TAXII-Protocol': 'urn:taxii.mitre.org:protocol:https:1.0'
    }

    try:
        response_message_poll = requests.post(url, proxies=False, verify=False, headers=head,
                                              data=taxii11.discovery_request(), auth=(taxii_user, taxii_password), timeout=3)
        if response_message_poll.status_code == 401:
            return "user/psw wrong"
        elif response_message_poll.status_code == 200:
            return "sucess"
        else:
            return "connection failed"
    except Exception as e:
        return "connection failed"

