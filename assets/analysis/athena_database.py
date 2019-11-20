import boto3
import os
import time
import copy
from time import sleep

from analysis.template_loader import TemplateLoader
from root import PROJECT_DIR

ATHENA_QUERIES_DIR = os.path.join(PROJECT_DIR, 'analysis/athena_queries')


class AthenaQueryError(Exception):
    pass


def wait_for_query_completion(client, query_execution_id, timeout=20):
    response = client.get_query_execution(QueryExecutionId=query_execution_id)
    query_state = response['QueryExecution']['Status']['State']
    sleep(0.5)
    start_time = time.time()
    while True:
        if query_state == 'SUCCEEDED':
            break
        if query_state == 'FAILED':
            raise AthenaQueryError(response)
        if (time.time() - start_time) > timeout:
            raise TimeoutError('Athena query execution timeout')
        sleep(0.5)
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_state = response['QueryExecution']['Status']['State']


def run_query_on_athena(client, template_loader, query_template, config, with_execution_context=False):
    query = template_loader.load_from_file(
        query_template,
        database_name=config['athena_database_name'],
        curated_bucket_name=config['curated_bucket_name'],
        customers_curated_dir=config['customers_curated_dir'],
        orders_curated_dir=config['orders_curated_dir'],
        demographics_curated_dir=config['demographics_curated_dir'],
        products_curated_dir=config['products_curated_dir']
    )
    output_location = 's3://{}/database-setup/{}'.format(
        config['athena_query_results_bucket_name'],
        os.path.basename(query_template)
    )
    query_config = dict(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': output_location,
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
    if with_execution_context:
        query_config['QueryExecutionContext'] = {
            'Database': config['athena_database_name']
        }
    response = client.start_query_execution(**query_config)
    query_execution_id = response['QueryExecutionId']
    wait_for_query_completion(client, query_execution_id)


def _run_athena_queries(config, query_templates):
    client = boto3.client('athena', region_name=config['region_name'])
    template_loader = TemplateLoader(ATHENA_QUERIES_DIR)

    templates_without_context = {'drop_database.sql', 'create_database.sql'}

    for query_template in query_templates:
        with_execution_context = query_template not in templates_without_context
        run_query_on_athena(client, template_loader, query_template, config, with_execution_context)


def configure_athena(config):
    query_templates = [
        'drop_database.sql',
        'create_database.sql',
        'register_demographics_table.sql',
        'repair_demographics_table.sql',
        'register_customers_table.sql',
        'repair_customers_table.sql',
        'register_orders_table.sql',
        'repair_orders_table.sql',
        'register_products_table.sql',
        'repair_products_table.sql'
    ]
    _run_athena_queries(config, query_templates)


def drop_spectrum_data_catalog(config):
    conf = copy.deepcopy(config)
    conf['athena_database_name'] = conf['redshift_external_database_name']
    query_templates = [
        'drop_database.sql'
    ]
    _run_athena_queries(conf, query_templates)
