import imp
import requests as rq
from consntants import DBREPO_TEST_INSTANCE
from query import Query
import pandas as pd

class Client:

    def __init__(self, username : str = None, password : str = None, url : str = DBREPO_TEST_INSTANCE) -> None:
        self.endpoint = f'{url}/api'
        self.__auth(username, password)


    def __auth(self, username : str, password : str) -> None:

        url = f'{self.endpoint}/auth'
        json = { 'username' : username, 'password' : password }

        res = rq.post(url, json=json, verify=False)
        self.token = self.__check_auth_res(res.json())
        self.token = 'Bearer ' + self.token


    def __check_auth_res(self, res: dict) -> str:

        if 'error' in res or 'token' not in res:
            if res['status'] == 401:
                raise ValueError('Authentication failed - username or password are wrong')
            else:
                raise ValueError('Authentication failed')

        return res['token']


    def query_by_pid(self, pid) -> Query:

        url = pid
        if isinstance(pid, int) or (isinstance(pid,str) and pid.isnumeric()):
            url = f'{self.endpoint}/pid/{pid}'

        res = rq.get(url, headers=self.__header(), verify=False)
        data = res.json()

        return self.query(data['cid'], data['dbid'], data['qid'])


    def query(self, cid: int, dbid: int, qid: int) -> Query:

        url = f'{self.endpoint}/container/{cid}/database/{dbid}/query/{qid}'

        res = rq.put(url, headers=self.__header(), verify=False)
        data = res.json()
        print(data)
        print(pd.DataFrame(data['result']))

    def __header(self):
        return {'Authorization': self.token}


client = Client(username='jtaha', password='fair12345')
client.query_by_pid('3')