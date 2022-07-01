
import requests
import time
from requests import Session
import json




class MetabaseStatsApi:
    _url: str = None
    _user: str = None
    _password: str = None

    def __init__(self, url: str, user: str, password: str) -> None:
        """

        Your description goes here

        :param url: Metabase api url
        :param user: Metabase username
        :param password: Metabase password

        :return: new MetabaseApi instance
        """
        self._url = url
        self._user = user
        self._password = password

    @property
    def url(self) -> str:
        return self._url.strip("/")

    @property
    def user(self) -> str:
        return self._user

    @property
    def password(self) -> str:
        return self._password

    @property
    def session(self) -> Session:
        payload = dict(username=self.user,
                       password=self.password)

        response = requests.post(f"{self.url}/api/session",
                                 data=json.dumps(payload),
                                 headers={"Content-Type": "application/json"})

        response.raise_for_status()

        json_body = response.json()

        json_body["X-Metabase-Session"] = json_body.pop("id")
        json_body["Content-Type"] = "application/json"

        session = requests.Session()

        session.headers.update(json_body)

        return session



    def _get_data(self, endpoint, params=None):
        print(f"{self.url}/api/{endpoint}")
        res = self.session.get(f"{self.url}/api/{endpoint}", params=params)
        request_time = time.time()
        res_json = res.json()
        #print(json.dumps(res_json,        indent = 4, sort_keys = True))

        # if list
        if isinstance(res_json, list):
            data = res_json
        else:
            if 'data' in res_json:
                data = res_json.get('data')
            else:
                data = [res_json]
        #print(data)
        print(type(data))
        print(data)

        #add metadata
        for d in data:
            d['endpoint'] = f"{self.url}/api/{endpoint}"
            d['request_time'] = request_time
            d['request_params'] = str(params)

        return data



    def _get_database_ids(self):
        databases = self._get_data('database')
        database_ids = [d['id'] for d in databases]
        return database_ids


    def _get_fields_endpoints(self):
        return [f"database/{id}/fields" for id in self._get_database_ids()]


    def get_rss_data(self):
        """ logs like reading cards or dashboards
        avtivity endpoint returns stuff like card edits
                :return:
        """
        endpoints = [dict(endpoint='activity', table='activity'),
                     dict(endpoint='util/logs', table='logs'),
                     ]

        for endpoint in endpoints:
            print(endpoint)
            data = self._get_data(endpoint.get('endpoint'), params=endpoint.get('params'))
            for row in data:
                yield endpoint.get('table'), row


    def get_stateful_data(self):
        """
        get the stateful configured dashboard. Item creation times,
        :return:
        """
        endpoints = [dict(endpoint='util/stats', table='stats'),
                     dict(endpoint='card', table='cards'),
                     dict(endpoint='collection', table='collections'),
                     dict(endpoint='dashboard', table='dashboards'),
                     dict(endpoint='database', table='databases'),
                     dict(endpoint='metric', table='metrics'),
                     dict(endpoint='pulse', table='pulses'),
                     dict(endpoint='table', table='tables'),
                     dict(endpoint='segment', table='segments'),
                     dict(endpoint='user', table='users', params={'status': 'all'}
                      )
                    ]

        # get fields endpoints, does a call to get db ids.
        for p in self._get_fields_endpoints():
            endpoints.append(dict(endpoint=p, table='fields'))

        for endpoint in endpoints:
            print(endpoint)
            data = self._get_data(endpoint.get('endpoint'), params=endpoint.get('params'))
            for row in data:
                yield endpoint.get('table'), row


