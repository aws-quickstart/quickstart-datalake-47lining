import bisect
import datetime
import json
import random
from time import sleep

import boto3
from botocore.exceptions import ClientError
import itertools
import functools
import logging


PRODUCT_CATEGORY_POPULARITY = {
    'Clothing_shoes_and_accessories': 44,
    'Electronics': 34,
    'Beauty_and_personal_care_items': 29,
    'Books_music_and_movies': 44,
    'Flowers_and_gifts': 14
}

PRODUCT_CATEGORY_SKU_PREFIX = {
    'Clothing_shoes_and_accessories': 'CS',
    'Electronics': 'ET',
    'Beauty_and_personal_care_items': 'BT',
    'Books_music_and_movies': 'BO',
    'Flowers_and_gifts': 'FG'
}

PRODUCT_CATEGORY_SKU_EXP_DISTRIBUTION = {
    'Clothing_shoes_and_accessories': 1/3,
    'Electronics': 1/4,
    'Beauty_and_personal_care_items': 1/5,
    'Books_music_and_movies': 1/6,
    'Flowers_and_gifts': 1/2
}

PRODUCT_CATEGORY_PRICE_DISTRIBUTIONS = {
    'Electronics': {'mean': 500, 'std': 50},
    'Clothing_shoes_and_accessories': {'mean': 120, 'std': 10},
    'Beauty_and_personal_care_items': {'mean': 50, 'std': 5},
    'Books_music_and_movies': {'mean': 30, 'std': 3},
    'Flowers_and_gifts': {'mean': 20, 'std': 2}
}


log = logging.getLogger(__name__)


def weighted_choice(values, cum_weights):
    total = cum_weights[-1]
    return values[bisect.bisect(cum_weights, random.random() * total)]


def make_weighted_generator(weight_dict):
    elements, weights = zip(*weight_dict.items())
    cum_weights = list(itertools.accumulate(weights))
    return functools.partial(weighted_choice, elements, cum_weights)


def generate_sku(product_category):
    scale = PRODUCT_CATEGORY_SKU_EXP_DISTRIBUTION[product_category]
    prefix = PRODUCT_CATEGORY_SKU_PREFIX[product_category]
    num = (1 + round(random.expovariate(scale))) * 9973
    return '{}-{}'.format(prefix, num)


def generate_price(product_category):
    dist_data = PRODUCT_CATEGORY_PRICE_DISTRIBUTIONS[product_category]
    return random.normalvariate(dist_data['mean'], dist_data['std'])


def make_orders_generator(customers_data_path):

    def generate_order():
        customer_id = generate_customer_id() if random.random() > 0.05 else None
        order = {
            'customer_id': customer_id,
            'order_date': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        product_category = generate_product_category()
        order['sku'] = generate_sku(product_category)
        order['price'] = generate_price(product_category)
        return order

    with open(customers_data_path) as customers_file:
        customer_to_weight = json.load(customers_file)
    generate_product_category = make_weighted_generator(PRODUCT_CATEGORY_POPULARITY)
    generate_customer_id = make_weighted_generator(customer_to_weight)

    return generate_order


def generate_data_to_kinesis(config):
    firehose_client = boto3.client('firehose', region_name=config['region_name'])
    generate_order = make_orders_generator(config['generator_customers_path'])
    sleep_interval = float(config['generator_sleep_interval'])
    chunk_size = int(config['generator_chunk_size'])
    for _ in range(int(config['generator_events_count']) // chunk_size):
        sleep(sleep_interval)
        records = [{'Data': json.dumps(generate_order())} for _ in range(chunk_size)]
        try:
            firehose_client.put_record_batch(
                DeliveryStreamName=config['orders_stream_name'],
                Records=records
            )
            log.info('Sending {} events'.format(chunk_size))
        except ClientError as e:
            log.error(e.response)
