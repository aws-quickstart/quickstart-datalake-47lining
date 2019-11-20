import boto3


def create_orders_application_config(app_name, input_in_app_stream_prefix, input_firehose_arn,
                                     output_streams_mapping, role_arn, app_code):
    application_config = dict(
        ApplicationName=app_name,
        Inputs=[
            {
                'NamePrefix': input_in_app_stream_prefix,
                'KinesisFirehoseInput': {
                    'ResourceARN': input_firehose_arn,
                    'RoleARN': role_arn
                },
                'InputParallelism': {
                    'Count': 1
                },
                'InputSchema': {
                    'RecordFormat': {
                        'RecordFormatType': 'JSON',
                        'MappingParameters': {
                            'JSONMappingParameters': {
                                'RecordRowPath': '$'
                            }
                        }
                    },
                    'RecordEncoding': 'UTF-8',
                    'RecordColumns': [
                        {
                            'Name': 'order_date',
                            'Mapping': '$.order_date',
                            'SqlType': 'TIMESTAMP'
                        },
                        {
                            'Name': 'customer_id',
                            'Mapping': '$.customer_id',
                            'SqlType': 'VARCHAR(64)'
                        },
                        {
                            'Name': 'sku',
                            'Mapping': '$.sku',
                            'SqlType': 'VARCHAR(64)'
                        },
                        {
                            'Name': 'price',
                            'Mapping': '$.price',
                            'SqlType': 'DECIMAL'
                        }
                    ]
                }
            }
        ],
        ApplicationCode=app_code
    )
    outputs = []
    for output_in_app_stream_name, output_firehose_arn in output_streams_mapping.items():
        outputs.append(
            {
                'Name': output_in_app_stream_name,
                'KinesisFirehoseOutput': {
                    'ResourceARN': output_firehose_arn,
                    'RoleARN': role_arn
                },
                'DestinationSchema': {
                    'RecordFormatType': 'JSON'
                }
            }
        )
    application_config['Outputs'] = outputs
    return application_config


def create_clean_orders_app_config(app_name, input_firehose_arn, output_streams_mapping, role_arn):
    return create_orders_application_config(
        app_name=app_name,
        input_in_app_stream_prefix='ORDERS_STREAM',
        input_firehose_arn=input_firehose_arn,
        output_streams_mapping=output_streams_mapping,
        role_arn=role_arn,
        app_code="""
            CREATE STREAM "CLEAN_ORDERS_STREAM" (
                "customer_id" VARCHAR(64),
                "sku" VARCHAR(64),
                "price" DECIMAL,
                "order_date" timestamp
            );

            CREATE PUMP "STREAM_PUMP" AS INSERT INTO "CLEAN_ORDERS_STREAM"
            SELECT "customer_id", "sku", "price", "order_date"
            FROM "ORDERS_STREAM_001"
            WHERE "customer_id" IS NOT NULL;
        """
    )


def create_aggregate_orders_app_config(app_name, input_firehose_arn, output_streams_mapping, role_arn):
    return create_orders_application_config(
        app_name=app_name,
        input_in_app_stream_prefix='CLEAN_ORDERS_STREAM',
        input_firehose_arn=input_firehose_arn,
        output_streams_mapping=output_streams_mapping,
        role_arn=role_arn,
        app_code="""
            CREATE STREAM "REVENUE_BY_STATE" (
                "state" VARCHAR(64),
                "revenue" DECIMAL,
                "timestamp" VARCHAR(30)
            );


            CREATE PUMP "STREAM_PUMP1" AS INSERT INTO "REVENUE_BY_STATE"
            SELECT STREAM
                "state",
                SUM("price") AS "revenue",
                TIMESTAMP_TO_CHAR('YYYY-MM-dd''T''HH:mm:ss', FLOOR(MONOTONIC("order_date") TO MINUTE)) AS "timestamp"
            FROM "CUSTOMERS"
            JOIN "CLEAN_ORDERS_STREAM_001"
            ON "CUSTOMERS"."customer_id" = "CLEAN_ORDERS_STREAM_001"."customer_id"
            GROUP BY FLOOR(MONOTONIC("order_date") TO MINUTE), "state";

            CREATE STREAM "TOP_SKU" (
                "sku" VARCHAR(64),
                "sku_count" BIGINT,
                "timestamp" VARCHAR(30)
            );

            CREATE PUMP "STREAM_PUMP2" AS INSERT INTO "TOP_SKU"
            SELECT STREAM
                *,
                TIMESTAMP_TO_CHAR('YYYY-MM-dd''T''HH:mm:ss', CURRENT_ROW_TIMESTAMP) AS "timestamp"
            FROM TABLE (TOP_K_ITEMS_TUMBLING(
                CURSOR(SELECT STREAM "sku" FROM "CLEAN_ORDERS_STREAM_001"),
                    'sku',
                    10,
                    60
                )
            );
        """
    )


def create_customers_reference_table_config(app_name, current_app_version, bucket_arn, bucket_key, role_arn):
    return dict(
        ApplicationName=app_name,
        CurrentApplicationVersionId=current_app_version,
        ReferenceDataSource={
            'TableName': 'CUSTOMERS',
            'S3ReferenceDataSource': {
                'BucketARN': bucket_arn,
                'FileKey': bucket_key,
                'ReferenceRoleARN': role_arn
            },
            'ReferenceSchema': {
                'RecordFormat': {
                    'RecordFormatType': 'CSV',
                    'MappingParameters': {
                        'CSVMappingParameters': {
                            'RecordRowDelimiter': '\n',
                            'RecordColumnDelimiter': ','
                        }
                    }
                },
                'RecordEncoding': 'UTF-8',
                'RecordColumns': [
                    {
                        'Name': 'customer_id',
                        'SqlType': 'VARCHAR(64)'
                    },
                    {
                        'Name': 'first_name',
                        'SqlType': 'VARCHAR(64)'
                    },
                    {
                        'Name': 'last_name',
                        'SqlType': 'VARCHAR(64)'
                    },
                    {
                        'Name': 'region',
                        'SqlType': 'VARCHAR(64)'
                    },
                    {
                        'Name': 'state',
                        'SqlType': 'VARCHAR(64)'
                    },
                    {
                        'Name': 'cbgid',
                        'SqlType': 'VARCHAR(64)'
                    }
                ]
            }
        }
    )


def create_clean_orders_app(client, app_name, config):
    app_config = create_clean_orders_app_config(
        app_name,
        input_firehose_arn=config['orders_stream_arn'],
        output_streams_mapping={
            'CLEAN_ORDERS_STREAM': config['clean_orders_stream_arn']
        },
        role_arn=config['kinesis_analytics_role_arn']
    )
    client.create_application(**app_config)


def create_aggregate_orders_app(client, app_name, config):
    app_config = create_aggregate_orders_app_config(
        app_name=app_name,
        input_firehose_arn=config['clean_orders_stream_arn'],
        output_streams_mapping={
            'REVENUE_BY_STATE': config['revenue_by_state_stream_arn'],
            'TOP_SKU': config['top_sku_stream_arn']
        },
        role_arn=config['kinesis_analytics_role_arn']
    )
    client.create_application(**app_config)
    app_description = client.describe_application(ApplicationName=app_name)
    current_app_version = app_description['ApplicationDetail']['ApplicationVersionId']
    reference_data_config = create_customers_reference_table_config(
        app_name=app_name,
        current_app_version=current_app_version,
        bucket_arn=config['curated_bucket_arn'],
        bucket_key=config['customers_curated_path'],
        role_arn=config['kinesis_analytics_role_arn']
    )
    client.add_application_reference_data_source(**reference_data_config)


def start_application(client, app_name):
    app_description = client.describe_application(ApplicationName=app_name)
    input_id = app_description['ApplicationDetail']['InputDescriptions'][0]['InputId']
    client.start_application(
        ApplicationName=app_name,
        InputConfigurations=[
            {
                'Id': input_id,
                'InputStartingPositionConfiguration': {
                    'InputStartingPosition': 'NOW'
                }
            },
        ]
    )


def create_kinesis_apps(config):
    client = boto3.client('kinesisanalytics', region_name=config['region_name'])
    response = client.list_applications()
    application_names = {summary['ApplicationName'] for summary in response['ApplicationSummaries']}
    app_name = 'clean-orders-app'
    if app_name not in application_names:
        create_clean_orders_app(client, app_name, config)
        start_application(client, app_name)
    app_name = 'aggregate-orders-app'
    if app_name not in application_names:
        create_aggregate_orders_app(client, app_name, config)
        start_application(client, app_name)
