#!/usr/bin/env python3

import argparse
import functools
import io
import os
import sys
import json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources

from jsonschema import validate
import singer

import httplib2

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

from google.cloud import bigquery
from google.cloud.bigquery import Dataset
from google.cloud.bigquery import SchemaField
from google.api_core import exceptions

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = 'https://www.googleapis.com/auth/bigquery'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Singer BigQuery Target'

StreamMeta = collections.namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """

    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'bigquery.googleapis.com-singer-target.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def build_schema(schema):
    SCHEMA = []
    for key in schema['properties'].keys():
        schema_name = key
        schema_type = "STRING"
        schema_mode = "NULLABLE"
        schema_fields = None

        if type(schema['properties'][key]['type']) is list:
            if schema['properties'][key]['type'][0] == "null":
                schema_mode = 'NULLABLE'
            else:
                schema_mode = 'required'
            schema_type = schema['properties'][key]['type'][1]
        else:
            schema_type = schema['properties'][key]['type']
            if schema_type == schema['properties'][key]['type'] == "array":
                schema_mode = "repeated"
                if "items" in schema['properties'][key]:
                    schema_fields = build_schema(schema['properties'][key])

        if schema_type == "string":
            if "format" in schema['properties'][key]:
                if schema['properties'][key]['format'] == "date-time":
                    schema_type = "timestamp"

        SCHEMA.append(SchemaField(schema_name, schema_type, schema_mode, schema_fields))

    return SCHEMA

def persist_lines(project_id, dataset_id, table_id, lines):
    state = None
    schemas = {}
    key_properties = {}

    headers_by_stream = {}
    
    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]
            validate(msg.record, schema)

            bigquery_client = bigquery.Client(project=project_id)

            dataset_ref = bigquery_client.dataset(dataset_id)
            dataset = Dataset(dataset_ref)

            try:
                dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
            except exceptions.Conflict:
                pass

            table_ref = dataset.table(table_id)
            table_schema = build_schema(schema)

            table = bigquery.Table(table_ref, schema=table_schema)
            try:
                table = bigquery_client.create_table(table)
            except exceptions.Conflict:
                pass

            rows = [msg.record]
            errors = bigquery_client.create_rows(table, rows) 

            if not errors:
                print('Loaded 1 row into {}:{}'.format(dataset_id, table_id))
            else:
                print('Errors:')
                pprint(errors)

            state = None
        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value
        elif isinstance(msg, singer.SchemaMessage):
            schemas[msg.stream] = msg.schema
            key_properties[msg.stream] = msg.key_properties
        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass
        else:
            raise Exception("Unrecognized message {}".format(msg))

    #print("Schemas: ", schemas[list(schemas.keys())[0]]['properties'])
    #print("\n\n")
    #print("Schema keys: ", schemas.keys())
    return state


def collect():
    try:
        version = pkg_resources.get_distribution('target-bigquery').version
        conn = http.client.HTTPSConnection('collector.stitchdata.com', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-bigquery',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')

        
def main():
    with open(flags.config) as input:
        config = json.load(input)
        
    """
    if not config.get('disable_collection', False):
        logger.info('Sending version information to stitchdata.com. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=collect).start()
    """

    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest')
    service = discovery.build('bigquery', 'v2', http=http,
                              discoveryServiceUrl=discoveryUrl)
    """

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    #state = persist_lines(service, config['project_id'], config['dataset_id'], config['table_id'], input)
    state = persist_lines(config['project_id'], config['dataset_id'], config['table_id'], input)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()


"""
schema:  {'properties': {'consult_cost': {'inclusion': 'available', 'minimum': -2147483648, 'maximum': 2147483647, 'type': ['null', 'integer']}, 'primary_address_id': {'inclusion': 'available', 'minimum': -2147483648, 'maximum': 2147483647, 'type': ['null', 'integer']}, 'is_core_override': {'inclusion': 'available', 'type': ['null', 'boolean']}, 'primary_specialty_id': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'last_change_by': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'website_vendor': {'inclusion': 'available', 'maxLength': 50, 'type': ['null', 'string']}, 'list_flags': {'inclusion': 'available', 'maxLength': 50, 'type': ['null', 'string']}, 'certification': {'inclusion': 'available', 'maxLength': 65535, 'type': ['null', 'string']}, 'postdoc_training': {'inclusion': 'available', 'maxLength': 65535, 'type': ['null', 'string']}, 'city': {'inclusion': 'available', 'maxLength': 50, 'type': ['null', 'string']}, 'auto_publish': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'address': {'inclusion': 'available', 'maxLength': 100, 'type': ['null', 'string']}, 'began_aesthetic_medicine': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'website': {'inclusion': 'available', 'maxLength': 200, 'type': ['null', 'string']}, 'education': {'inclusion': 'available', 'maxLength': 65535, 'type': ['null', 'string']}, 'linked_url': {'inclusion': 'available', 'maxLength': 200, 'type': ['null', 'string']}, 'anonymous_votes': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'primary_twilio_id': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'zipcode': {'inclusion': 'available', 'maxLength': 50, 'type': ['null', 'string']}, 'initial_contact_date': {'inclusion': 'available', 'maxLength': 50, 'type': ['null', 'string']}, 'modified': {'inclusion': 'available', 'format': 'date-time', 'type': ['null', 'string']}, 'additional_addresses': {'inclusion': 'available', 'maxLength': 500, 'type': ['null', 'string']}, 'business': {'inclusion': 'available', 'maxLength': 100, 'type': ['null', 'string']}, 'gplus': {'inclusion': 'available', 'maxLength': 200, 'type': ['null', 'string']}, 'last_name': {'inclusion': 'available', 'maxLength': 50, 'type': ['null', 'string']}, 'status': {'inclusion': 'available', 'type': ['null', 'boolean']}, 'enable_activity_stream': {'inclusion': 'available', 'type': ['null', 'boolean']}, 'pro_purchase_date': {'inclusion': 'available', 'format': 'date-time', 'type': ['null', 'string']}, 'awards': {'inclusion': 'available', 'maxLength': 65535, 'type': ['null', 'string']}, 'uri': {'inclusion': 'available', 'maxLength': 128, 'type': ['null', 'string']}, 'inactive': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'realcare_promise': {'inclusion': 'available', 'type': ['null', 'boolean']}, 'tier': {'inclusion': 'available', 'maxLength': 14, 'type': ['null', 'string']}, 'statement': {'inclusion': 'available', 'maxLength': 65535, 'type': ['null', 'string']}, 'user_id': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'country': {'inclusion': 'available', 'maxLength': 50, 'type': ['null', 'string']}, 'photo': {'inclusion': 'available', 'maxLength': 128, 'type': ['null', 'string']}, 'hide_gplus_msg': {'inclusion': 'available', 'type': ['null', 'boolean']}, 'directory_link': {'inclusion': 'available', 'maxLength': 150, 'type': ['null', 'string']}, 'name': {'inclusion': 'available', 'maxLength': 60, 'type': ['null', 'string']}, 'consult_notes': {'inclusion': 'available', 'maxLength': 300, 'type': ['null', 'string']}, 'disclosures': {'inclusion': 'available', 'maxLength': 500, 'type': ['null', 'string']}, 'lead_email': {'inclusion': 'available', 'maxLength': 255, 'type': ['null', 'string']}, 'address_suite': {'inclusion': 'available', 'maxLength': 100, 'type': ['null', 'string']}, 'spotlight': {'inclusion': 'available', 'type': ['null', 'boolean']}, 'opinion_leader': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'clinical_privileges': {'inclusion': 'available', 'maxLength': 500, 'type': ['null', 'string']}, 'state': {'inclusion': 'available', 'maxLength': 50, 'type': ['null', 'string']}, 'primary_location_id': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'twitter_facebook': {'inclusion': 'available', 'type': ['null', 'boolean']}, 'advisor_user_id': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'gender': {'inclusion': 'available', 'maxLength': 10, 'type': ['null', 'string']}, 'rid': {'inclusion': 'automatic', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'salesforce_id': {'inclusion': 'available', 'maxLength': 15, 'type': ['null', 'string']}, 'phone': {'inclusion': 'available', 'maxLength': 100, 'type': ['null', 'string']}, 'id': {'inclusion': 'automatic', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'promote': {'inclusion': 'available', 'minimum': 0, 'maximum': 4294967295, 'type': ['null', 'integer']}, 'created': {'inclusion': 'available', 'format': 'date-time', 'type': ['null', 'string']}, 'premier_status': {'inclusion': 'available', 'maxLength': 20, 'type': ['null', 'string']}}, 'type': 'object'}
"""
