import json
import requests
import aiohttp
from datetime import datetime


class LogentriesConnection(object):
    API_URL = "https://rest.logentries.com"

    def __init__(self, key):
        self.key = key

    def get_logset_logs(self, logset_id):
        path = "/management/logsets/{0}".format(logset_id)
        response = self.get(path)

        logs = []
        for log in response['logset']['logs_info']:
            logs.append(log['id'])

        return logs

    async def get_logset_logs_async(self, logset_id):
        path = "/management/logsets/{0}".format(logset_id)
        response = await self.get_async(path)

        logs = []
        for log in response['logset']['logs_info']:
            logs.append(log['id'])

        return logs

    def _build_headers(self):
        return {'x-api-key': self.key}

    def get(self, path):
        url = "{0}{1}".format(self.API_URL, path)
        response = requests.get(url, headers=self._build_headers())
        return response.json()

    async def get_async(self, path):
        headers = self._build_headers()
        url = "{0}{1}".format(self.API_URL, path)

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.json()

    def post(self, path, query):
        headers = self._build_headers()
        path = "{0}{1}".format(self.API_URL, path)

        response = requests.post(
            path,
            json=query,
            headers=headers
        )

        return response.json()

        #TODO add optional 'tail' parameter
        # while response.status_code >= 200:
        #     if 'links' in response.json():
        #         continue_url = response.json()['links'][0]['href']
        #         response = requests.get(continue_url, headers=headers)
        #     else:
        #         return json.dumps(response.json(), indent=4)

    async def post_async(self, path, query):
        headers = self._build_headers()
        url = "{0}{1}".format(self.API_URL, path)

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=query, headers=headers) as response:
                return await response.json()

    def query(self, query):
        response = self._post("/query/logs", query)
        return json.loads(response)


class Query(object):

    def __init__(self):
        self._logs = None
        self._where = None
        self._groupby = None
        self._calculate = None
        self._interval = None

    def where(self, query_string):
        if self._where is not None:
            raise Exception('duplicate definition of \'where\' clause')

        self._where = query_string
        return self

    def and_(self, query_string):
        if self._where is None:
            raise Exception('\'where\' clause not declared')

        self._where += " AND {} ".format(query_string)
        return self

    def or_(self, query_string):
        if self._where is None:
            raise Exception('\'where\' clause not declared')

        self._where += " OR {} ".format(query_string)
        return self

    def groupby(self, field_name):
        if self._groupby is not None:
            raise Exception('duplicate definition of \'groupby\' clause')

        self._groupby = "groupby({})".format(field_name)
        return self

    def calculate(self, operation):
        if self._calculate is not None:
            raise Exception('duplicate definition of \'calculate\' clause')

        self._calculate = "calculate({})".format(operation)
        return self

    def interval(self, from_timestamp, to_timestamp):
        self._interval = {
            "from": from_timestamp,
            "to": to_timestamp
        }
        return self

    def logs(self, log_ids):
        self._logs = log_ids
        return self

    def to_string(self):
        where = self._where or ""
        groupby = self._groupby or ""
        calculate = self._calculate or ""

        stringified = "where({}) {} {}".format(
            where, groupby, calculate
        )

        return stringified.strip()

    def build(self):
        query = {
            "logs": self._logs,
            "leql": {
                "during": self._interval,
                "statement": self.to_string()
            }
        }

        return query


def get_timestamp(dt):
    dt_obj = datetime.strptime(dt, '%d/%m/%Y %H:%M:%S')
    millisec = int(dt_obj.timestamp() * 1000)
    return millisec
