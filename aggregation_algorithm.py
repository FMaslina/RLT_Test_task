import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()


async def connect_to_db():
    client = AsyncIOMotorClient(os.getenv("DB_ADDRESS"), int(os.getenv("DB_PORT")))
    db = client[os.getenv("DB_NAME")]
    collection = db[os.getenv("COLLECTION_NAME")]
    return collection


async def hour_aggregating(collection, start_date, end_date) -> dict:
    current_date = start_date
    hourly_labels = []

    while current_date <= end_date:
        hourly_labels.append(current_date)
        current_date += timedelta(hours=1)

    # Условия для агрегации
    pipeline_hour = [
        {
            '$match': {
                'dt': {'$gte': start_date, '$lte': end_date}
            }
        },
        {
            '$group': {
                '_id': {
                    'year': {'$year': '$dt'},
                    'month': {'$month': '$dt'},
                    'day': {'$dayOfMonth': '$dt'},
                    'hour': {'$hour': '$dt'}
                },
                'totalValue': {'$sum': '$value'}
            }
        },
        {
            '$sort': {'_id.year': 1, '_id.month': 1, '_id.day': 1, '_id.hour': 1}
        }
    ]

    # Добавляем нулевые значения в те дни, где нет данных
    result_hour = await collection.aggregate(pipeline_hour).to_list(None)
    hourly_data = {label: 0 for label in hourly_labels}

    for item in result_hour:
        label = datetime(item['_id']['year'], item['_id']['month'], item['_id']['day'], item['_id']['hour'], 0, 0)
        hourly_data[label] = item['totalValue']

    dataset = []
    labels = []
    for label, value in hourly_data.items():
        labels.append(label.strftime("%Y-%m-%dT%H:%M:%S"))
        dataset.append(value)

    response = {
        "dataset": dataset,
        "labels": labels
    }

    return response


async def day_aggregating(collection, start_date, end_date) -> dict:
    current_date = start_date
    daily_labels = []

    while current_date <= end_date:
        daily_labels.append(current_date)
        current_date += timedelta(days=1)

    pipeline_day = [
        {
            '$match': {
                'dt': {'$gte': start_date, '$lte': end_date}
            }
        },
        {
            '$group': {
                '_id': {
                    'year': {'$year': '$dt'},
                    'month': {'$month': '$dt'},
                    'day': {'$dayOfMonth': '$dt'},
                },
                'totalValue': {'$sum': '$value'}
            }
        },
        {
            '$sort': {'_id.year': 1, '_id.month': 1, '_id.day': 1, '_id.hour': 1}
        }
    ]

    result_day = await collection.aggregate(pipeline_day).to_list(None)
    daily_data = {label: 0 for label in daily_labels}

    for item in result_day:
        label = datetime(item['_id']['year'], item['_id']['month'], item['_id']['day'], 0, 0, 0)
        daily_data[label] = item['totalValue']

    dataset = []
    labels = []
    for label, value in daily_data.items():
        labels.append(label.strftime("%Y-%m-%dT%H:%M:%S"))
        dataset.append(value)

    response = {
        "dataset": dataset,
        "labels": labels
    }

    return response


async def month_aggregating(collection, start_date, end_date) -> dict:
    current_date = start_date
    monthly_labels = []

    while current_date <= end_date:
        monthly_labels.append(current_date)
        current_date += relativedelta(months=1)

    pipeline_month = [
        {
            '$match': {
                'dt': {'$gte': start_date, '$lte': end_date}
            }
        },
        {
            '$group': {
                '_id': {
                    'year': {'$year': '$dt'},
                    'month': {'$month': '$dt'},
                },
                'totalValue': {'$sum': '$value'}
            }
        },
        {
            '$sort': {'_id.year': 1, '_id.month': 1, '_id.day': 1, '_id.hour': 1}
        }
    ]

    result_month = await collection.aggregate(pipeline_month).to_list(None)
    monthly_data = {label: 0 for label in monthly_labels}

    for item in result_month:
        label = datetime(item['_id']['year'], item['_id']['month'], 1, 0, 0, 0)
        monthly_data[label] = item['totalValue']

    dataset = []
    labels = []
    for label, value in monthly_data.items():
        labels.append(label.strftime("%Y-%m-%dT%H:%M:%S"))
        dataset.append(value)

    response = {
        "dataset": dataset,
        "labels": labels
    }

    return response


async def aggregate(data):
    start_date_str = data['dt_from']
    start_date = datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%S')
    end_date_str = data['dt_upto']
    end_date = datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%S')
    group_type = data['group_type']

    collection = await connect_to_db()

    if group_type == "hour":
        return await hour_aggregating(collection, start_date, end_date)
    elif group_type == "day":
        return await day_aggregating(collection, start_date, end_date)
    elif group_type == "month":
        return await month_aggregating(collection, start_date, end_date)
