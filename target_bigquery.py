#!/usr/bin/env python3

import argparse
import io
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

from oauth2client import tools

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

        if isinstance(schema['properties'][key]['type'], list):
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

    rows = []

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

            rows.append(msg.record)

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

    errors = bigquery_client.create_rows(table, rows)

    if not errors:
        print('Loaded {} row(s) into {}:{}'.format(len(rows), dataset_id, table_id))
    else:
        print('Errors:')
        pprint(errors)



    return state

def collect():
    try:
        version = pkg_resources.get_distribution('target-bigquery').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-csv',
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

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    state = persist_lines(config['project_id'], config['dataset_id'], config['table_id'], input)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
