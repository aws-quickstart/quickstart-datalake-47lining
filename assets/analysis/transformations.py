import os
from concurrent.futures import ThreadPoolExecutor
from zipfile import ZipFile

import boto3
import datetime
import functools

import itertools

from analysis.template_loader import TemplateLoader
from analysis.redshift import RedshiftConnection, RedshiftManager
from analysis.athena_database import drop_spectrum_data_catalog
from root import PROJECT_DIR

REDSHIFT_SQL_DIR = os.path.join(PROJECT_DIR, 'analysis/redshift_sql')
MANIFEST_TEMPLATES_DIR = os.path.join(PROJECT_DIR, 'analysis/manifests')
DEMOGRAPHICS_DATA_DOWNLOAD_PATH = '/tmp/demographics.zip'
DEMOGRAPHICS_DATA_EXTRACTED_FILE_NAME = 'demographics_data.csv'
DEMOGRAPHICS_DATA_EXTRACT_DIR = '/tmp'
MANIFEST_TMP_DIR = '/tmp/'

ANALYSIS_TABLE_INFO = {
    'customer_lifetime_value': {
        's3_publish_prefix': 'customer',
        'columns': ['segment', 'segment_spend']
    },
    'orders_by_month': {
        's3_publish_prefix': 'orders',
        'columns': ['month_date', 'married_orders', 'single_orders', 'total_orders']
    },
    'product_category_distribution': {
        's3_publish_prefix': 'products',
        'columns': ['product_category', 'orders_count']
    },
    'sku_distribution': {
        's3_publish_prefix': 'sku',
        'columns': ['product_category', 'sku', 'sku_count']
    },
    'spend_distribution': {
        's3_publish_prefix': 'spend',
        'columns': ['customer_id', 'spend', 'marital_status', 'education_level']
    },
    'state_population': {
        's3_publish_prefix': 'state',
        'columns': ['state', 'population']
    }
}


def _make_redshift_manager(config):
    redshift_connection = RedshiftConnection(
        config['redshift_username'],
        config['redshift_password'],
        config['redshift_jdbc_url']
    )
    query_loader = TemplateLoader(REDSHIFT_SQL_DIR)
    return RedshiftManager(
        region_name=config['region_name'],
        redshift_role_arn=config['redshift_role_arn'],
        redshift_connection=redshift_connection,
        query_loader=query_loader
    )


def _make_bucket(bucket_name):
    # It is recommended to create a resource instance for each thread in a multithreaded applications
    # https://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing
    session = boto3.session.Session()
    return session.resource('s3').Bucket(bucket_name)


def _run_in_parallel(job_generator, max_workers):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(job) for job in job_generator]
    for future in futures:
        exception = future.exception()
        if exception is not None:
            raise exception


def _transform_demographics(config):
    submissions_bucket = _make_bucket(config['submissions_bucket_name'])
    curated_bucket = _make_bucket(config['curated_bucket_name'])
    submissions_bucket.download_file(config['demographics_submission_path'], DEMOGRAPHICS_DATA_DOWNLOAD_PATH)
    with ZipFile(DEMOGRAPHICS_DATA_DOWNLOAD_PATH) as zipped:
        zipped.extract(DEMOGRAPHICS_DATA_EXTRACTED_FILE_NAME, DEMOGRAPHICS_DATA_EXTRACT_DIR)
    extracted_path = os.path.join(DEMOGRAPHICS_DATA_EXTRACT_DIR, DEMOGRAPHICS_DATA_EXTRACTED_FILE_NAME)
    curated_bucket.upload_file(extracted_path, config['demographics_curated_path'])


def _copy_customers(config):
    curated_bucket = _make_bucket(config['curated_bucket_name'])
    copy_source = {
        'Bucket': config['submissions_bucket_name'],
        'Key': config['customers_submission_path']
    }
    curated_bucket.copy(copy_source, config['customers_curated_path'])


def _copy_orders(config, bucket_key):
    curated_bucket = _make_bucket(config['curated_bucket_name'])
    copy_source = {
        'Bucket': config['submissions_bucket_name'],
        'Key': bucket_key
    }
    path = os.path.join(os.path.dirname(config['orders_curated_path']), os.path.basename(bucket_key))
    curated_bucket.copy(copy_source, path)


def _copy_products(config, bucket_key):
    curated_bucket = _make_bucket(config['curated_bucket_name'])
    copy_source = {
        'Bucket': config['submissions_bucket_name'],
        'Key': bucket_key
    }
    file_name = os.path.basename(bucket_key)
    year = file_name[-4:]
    partition = 'dt={}-12-31/products.csv'.format(year)
    destination_path = os.path.join(config['products_curated_dir'], partition)
    curated_bucket.copy(copy_source, destination_path)


def _generate_submission_transformations(config):
    submissions_bucket = _make_bucket(config['submissions_bucket_name'])

    yield from (
        functools.partial(_transform_demographics, config),
        functools.partial(_copy_customers, config)
    )

    for obj in submissions_bucket.objects.filter(Prefix=config['orders_submission_path']):
        yield functools.partial(_copy_orders, config, bucket_key=obj.key)

    for obj in submissions_bucket.objects.filter(Prefix=config['products_submission_path']):
        yield functools.partial(_copy_products, config, bucket_key=obj.key)


def _create_customers_table(config):
    redshift_manager = _make_redshift_manager(config)
    redshift_manager.execute_from_file(
        'create_customers_curated_dataset.sql',
        curated_bucket_name=config['curated_bucket_name'],
        customers_curated_path=config['customers_curated_path']
    )


def _create_orders_table(config):
    redshift_manager = _make_redshift_manager(config)
    redshift_manager.execute_from_file(
        'create_orders_curated_dataset.sql',
        curated_bucket_name=config['curated_bucket_name'],
        orders_curated_path=config['orders_curated_path']
    )


def _create_external_tables(config):
    drop_spectrum_data_catalog(config)
    redshift_manager = _make_redshift_manager(config)
    redshift_manager.execute_from_file(
        'create_external_schema.sql',
        external_schema_name=config['redshift_external_schema_name'],
        external_database_name=config['redshift_external_database_name']
    )
    redshift_manager.execute_from_file(
        'create_external_tables.sql',
        curated_bucket_name=config['curated_bucket_name'],
        demographics_curated_dir=config['demographics_curated_dir'],
        demographics_partition=config['demographics_partition'],
        products_curated_dir=config['products_curated_dir'],
        external_schema_name=config['redshift_external_schema_name']
    )


def _generate_load_jobs(config):
    yield from (
        functools.partial(_create_customers_table, config),
        functools.partial(_create_orders_table, config),
        functools.partial(_create_external_tables, config)
    )


def create_and_load_curated_datasets(config):
    _run_in_parallel(_generate_submission_transformations(config), max_workers=20)
    _run_in_parallel(_generate_load_jobs(config), max_workers=3)


def _run_query(query_file_name, config, **kwargs):
    redshift_manager = _make_redshift_manager(config)
    redshift_manager.execute_from_file(query_file_name, **kwargs)


def _generate_analysis_jobs(config):
    analysis_query_files = [
        'create_product_category_distribution.sql',
        'create_spend_distribution.sql',
        'create_customer_lifetime_value.sql',
        'create_orders_by_month.sql',
        'create_sku_distribution.sql',
        'create_state_population.sql'
    ]
    for query_file in analysis_query_files:
        yield functools.partial(
            _run_query,
            query_file,
            config,
            external_schema_name=config['redshift_external_schema_name']
        )


def _unload_analysis_table_to_s3(config, bucket_name, table_name, path):
    redshift_manager = _make_redshift_manager(config)
    table_columns = ANALYSIS_TABLE_INFO[table_name]['columns']
    redshift_manager.execute_from_file(
        'publish_table_to_s3.sql',
        published_bucket_name=bucket_name,
        table_name=table_name,
        path=path,
        columns=table_columns
    )


def _generate_save_analysis_jobs(config):
    datestamp = datetime.datetime.now().strftime('%Y-%m-%d')
    for table_name in ANALYSIS_TABLE_INFO.keys():
        path = '{}/v=20170601/p=csv/dt={}'.format(table_name, datestamp)
        yield functools.partial(
            _unload_analysis_table_to_s3,
            config,
            bucket_name=config['curated_bucket_name'],
            table_name=table_name,
            path=path
        )


def run_redshift_analysis(config):
    _run_in_parallel(_generate_analysis_jobs(config), max_workers=6)
    _run_in_parallel(_generate_save_analysis_jobs(config), max_workers=6)


def _generate_publish_analysis_jobs(config):
    for table_name, table_info in ANALYSIS_TABLE_INFO.items():
        path = '{}/data'.format(table_info['s3_publish_prefix'])
        yield functools.partial(
            _unload_analysis_table_to_s3,
            config,
            bucket_name=config['published_bucket_name'],
            table_name=table_name,
            path=path
        )


def _generate_publish_manifest_jobs(config):
    template_loader = TemplateLoader(MANIFEST_TEMPLATES_DIR)
    for table_name, table_info in ANALYSIS_TABLE_INFO.items():
        data_path = '{}/data'.format(table_info['s3_publish_prefix'])
        manifest_str = template_loader.load_from_file(
            'manifest_template.mf',
            published_bucket_name=config['published_bucket_name'],
            path=data_path
        )
        manifest_tmp_path = os.path.join(MANIFEST_TMP_DIR, table_name)
        with open(manifest_tmp_path, 'w') as manifest_file:
            manifest_file.write(manifest_str)
        bucket = _make_bucket(config['published_bucket_name'])
        s3_path = '{}/manifest/{}.mf'.format(table_info['s3_publish_prefix'], table_name)
        yield functools.partial(bucket.upload_file, manifest_tmp_path, s3_path)


def publish_analysis_results(config):
    _run_in_parallel(
        itertools.chain(_generate_publish_manifest_jobs(config), _generate_publish_analysis_jobs(config)),
        max_workers=12
    )
