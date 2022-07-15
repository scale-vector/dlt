
import requests
import time
from requests import Session
import json




class MetabaseSource:
    _url: str = None
    _user: str = None
    _password: str = None

    """  A list of tables that can be passed to get_table_rows() to get an interator of rows

        Metabase publishes logs to a buffer that keeps a running window.
        depending on how many events are generated in your instance,
        you might want to schedule a read every few minutes or every few days.

        They are set to "append-only" mode, so deduplication will be done by you by your optimal cost scenario
    """
    event_window_tables = ['activity', 'logs']

    """ returns a list of available tasks (to get data sets).
        pass them to get_endpoint_rows() to get an iterator of rows.
        These are stateful and should be replaced
    """
    stateful_tables = ['stats', 'cards', 'collections', 'dashboards', 'databases',
                  'metrics', 'pulses', 'tables', 'segments', 'users']


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
        #print(json.dumps(res_json, indent = 4, sort_keys = True))

        # if list
        if isinstance(res_json, list):
            data = res_json
        else:
            if 'data' in res_json:
                data = res_json.get('data')
            else:
                data = [res_json]

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

    def get_field_data(self):
        for p in self._get_fields_endpoints():
            data = self._get_data(p)
            for row in data:
                yield row

    def get_table_rows(self, table):
        """this function returns an interator of rows"""
        #if it's a simple call, we return the result in a list
        simple_endpoints = [dict(endpoint='util/stats', table='stats'),
                     dict(endpoint='card', table='cards'),
                     dict(endpoint='collection', table='collections'),
                     dict(endpoint='dashboard', table='dashboards'),
                     dict(endpoint='database', table='databases'),
                     dict(endpoint='metric', table='metrics'),
                     dict(endpoint='pulse', table='pulses'),
                     dict(endpoint='table', table='tables'),
                     dict(endpoint='segment', table='segments'),
                     dict(endpoint='user', table='users', params={'status': 'all'}),
                     dict(endpoint='activity', table='activity'),
                     dict(endpoint='util/logs', table='logs'),
                     ]
        if table in [e['table'] for e in simple_endpoints]:
            data = self._get_data(simple_endpoints.get('endpoint'), params=simple_endpoints.get('params'))
            return data

        #for tables that need more calls, we return a generator
        if table == 'fields':
            return self.get_field_data()

