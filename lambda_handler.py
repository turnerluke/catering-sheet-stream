import itertools
import uuid
from decimal import Decimal
import datetime as dt

import pandas as pd

from ziki_helpers.aws.dynamodb import get_entire_table
from ziki_helpers.aws.s3 import dataframe_to_s3_with_date_partition
from ziki_helpers.gcloud.sheets import get_gsheet_as_df


def handler(event, context):

    # Get the entire locations table
    locations = get_entire_table('locations')

    # Get mapping from catering document location to id
    locations = list(itertools.chain.from_iterable(loc['info'] for loc in locations))
    loc_mapping = {loc['cateringDocName']: loc['id'] for loc in locations if loc.get('cateringDocName')}

    # Get the catering table
    df = get_gsheet_as_df('Master Catering Doc')[['Order Date', 'Gross Sales', 'Location', 'Taxes', 'Channel']]

    # Preprocess
    df['Order Date'] = pd.to_datetime(df['Order Date']).dt.date.astype(str)
    df['Gross Sales'] = pd.to_numeric(df['Gross Sales'].str.strip('$').str.replace(',', ''), errors='coerce').fillna(0)
    df['Taxes'] = pd.to_numeric(df['Taxes'].str.strip('$').str.replace(',', ''), errors='coerce').fillna(0)
    df['Location'] = df['Location'].str.strip()

    # Remove Toast Catering
    df = df.loc[df['Channel'] != 'ZIKI Catering']
    df = df.drop(columns=['Channel'])

    # Camel case columns
    df = df.rename(columns={'Order Date': 'businessDate', 'Gross Sales': 'gross', 'Location': 'location', 'Taxes': 'tax'})

    # Map Locations to ID
    df['location'] = df['location'].map(loc_mapping)

    # Fill info to match sales table format
    df['item'] = 'Catering'
    df['quantity'] = 1
    df['estimatedFulfillmentDate'] = None
    df['guid'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['diningOption'] = 'Catering Partners'

    # Write to S3
    for date in df['businessDate'].unique():
        date_obj = dt.datetime.strptime(date, '%Y-%m-%d').date()

        subset = df.loc[df['businessDate'] == date]
        dataframe_to_s3_with_date_partition(
            df=subset,
            bucket_name='ziki-analytics-tables',
            tablename='sales',
            date=date_obj,
            filename='cater'
        )


if __name__ == '__main__':
    handler(None, None)
    