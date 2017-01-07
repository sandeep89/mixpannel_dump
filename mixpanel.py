#! /usr/bin/env python
#
# Code to capture all the mixpannel event and properties
# store them into a mysql table
#
import base64
import json
import urllib
import urllib2
import string
import MySQLdb


class DBStore(object):

    def __init__(self, connection_properties):
        self.db = MySQLdb.connect(
            connection_properties["host"],
            connection_properties["username"],
            connection_properties["password"],
            connection_properties["database"])
        self.cursor = self.db.cursor()

    def get_event(self, event_name):
        self.cursor.execute("SELECT * from events where event_name ='" + event_name
                       + "';")
        data = cursor.fetchone()
        return data

    def insert_event(self, event_data):
        try:
            self.cursor.execute("INSERT into events(event_name, properties) values('"
                                + event_data['event']+"','"
                                + event_data['properties']
                                + "');")
            self.db.commit()
        except:
            self.db.rollback()

    def update_event(self, event_data):
        self.cursor.execute("UPDATE events set properties = "
                            + event_data["properties"] + " where "
                            + "event_name ='"+event_data['event']+"';")


class Mixpanel(object):

    DATAENDPOINT = 'https://data.mixpanel.com/api'
    ENDPOINT = 'https://mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_secret):
        self.api_secret = api_secret

    def request(self, methods, params, http_method='GET', format='json', data=False):

        params['format'] = format

        if data:
            request_url = '/'.join([self.DATAENDPOINT, str(self.VERSION)] + methods)
        else:
            request_url = '/'.join([self.ENDPOINT, str(self.VERSION)] + methods)
        if http_method == 'GET':
            data = None
            request_url = request_url + '/?' + self.unicode_urlencode(params)
        else:
            data = self.unicode_urlencode(params)

        headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=base64.b64encode(self.api_secret))}
        request = urllib2.Request(request_url, data, headers)
        response = urllib2.urlopen(request)
        return response.read()

    def unicode_urlencode(self, params):
        """
            Convert lists to JSON encoded strings, and correctly handle any
            unicode URL parameters.
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        return urllib.urlencode(
            [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
        )

if __name__ == '__main__':
    api = Mixpanel(api_secret='<mixpannel Key>')
    db_store = DBStore({
        "host": "localhost",
        "username": "root",
        "password": "password",
        "database": "mixpannel_events"
        })
    data = api.request(['events','names'],{
            "type":"unique",
            "limit":10000
        })
    data = json.loads(data)
    print data
    for event in data:
        print event
        properties = api.request(['events',
            'properties','top'],{
                "event": event,
                "limit": "100"
            })
        properties = json.loads(properties)
        print properties.keys()
        db_store.insert_event({
            "event" : event,
            "properties" : ','.join(properties.keys())
            })
        
