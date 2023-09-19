import requests
import pandas as pd
from datetime import datetime, timedelta

def extract_element(dict_obj):
    return dict_obj.get('niftyTitle')

def create_dict(keys, values):
    """Creates a dictionary from the given keys and values."""
    return dict(zip(keys, values))

def fetch_api_data(url, headers, params=None):
    """Fetch data from the API and return the JSON response."""
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Check if the request was successful
    return response.json()

def get_usd_eth_rate():
    api_endpoint = "https://api.niftygateway.com/v1/fxrates/"
    params = {"source_currency": 'ETH', "base_currency": 'USD', "order_by": "-created_at", "limit": 1}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    # Fetch data from the API endpoint
    data = fetch_api_data(api_endpoint, headers, params=params)
    return data

def calculate_nifty_gateway_data(data, time_range):
    total_volume = 0
    num_of_sales = 0
    floor_price = 0
    owners = 0
    len_data = len(data)
    if 'seven' == time_range:
        for i in range(0, len_data):
            total_volume += float(data[i]['sevenDayTotalVolume'])
            num_of_sales += int(data[i]['sevenDayNumTotalSales'])
            floor_price += float(data[i]['floorPrice'])
            owners += int(data[i]['numOwners'])
    else:
        for i in range(0, len_data):
            total_volume += float(data[i]['thirtyDayTotalVolume'])
            num_of_sales += int(data[i]['thirtyDayNumTotalSales'])
            floor_price += float(data[i]['floorPrice'])
            owners += int(data[i]['numOwners'])
        
    floor_price = floor_price / len_data

    rate_results  = get_usd_eth_rate()
    rate = rate_results['results'][0]['price']
    
    total_volume = total_volume / float(rate)
    floor_price = floor_price / float(rate)
    
    keys = [f'{time_range} days', 'Volume', 'No of  Sales', 'Average Floor Price', 'Owners']
    values = ['Nifty Gateway', total_volume, num_of_sales, floor_price, owners]
    result = create_dict(keys, values)
    
    return result

def get_nifty_gateway_data_by_page(page, time_range):
    # Define the API endpoint and parameters
    api_endpoint = "https://api.niftygateway.com/stats/rankings/"
    params = {"page": page, "page_size": 100, "sort": f'-{time_range}_day_total_volume'}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    # Fetch data from the API endpoint
    data = fetch_api_data(api_endpoint, headers, params=params)
    return data

def save_nifty_gateway_data_to_file(df, time_range):
    df.to_csv(f'nifty_gateway_{time_range}_day_data.csv', index=False)
    df.to_excel(f'nifty_gateway_{time_range}_day_data.xlsx', index=False)

def divide_rate(x, rate):
    return float(x) / rate

def get_nifty_gateway_data(time_range):
    """Get data from the Nifty Gateway API endpoint."""
    
    page_num = 1
    data = get_nifty_gateway_data_by_page(page_num, time_range)
    results = data['results']
    total_page = data['count'] // 100 + 1
    if 'seven' == time_range:
        total_page = 4
    else:
        total_page = 10
    for i in range(2, total_page + 1):
    # for i in range(2, 3):
        new_data = get_nifty_gateway_data_by_page(i, time_range)
        results += new_data['results']
    
    calculate_nifty_gateway_result = calculate_nifty_gateway_data(results, time_range)
    calculate_df = pd.DataFrame([calculate_nifty_gateway_result])

    df = pd.DataFrame(results)
    columns_to_keep = []
    if 'seven' == time_range:
        columns_to_keep = ['collection', 'sevenDayTotalVolume', 'sevenDayNumTotalSales', 'floorPrice', 'numOwners']
    else:
        columns_to_keep = ['collection', 'thirtyDayTotalVolume', 'thirtyDayNumTotalSales', 'floorPrice', 'numOwners']
    
    df = df[columns_to_keep]
    
    df['collection'] = df['collection'].apply(extract_element)

    rate_results  = get_usd_eth_rate()
    rate = float(rate_results['results'][0]['price'])

    df['floorPrice'] = df['floorPrice'].apply(lambda x: divide_rate(x, rate))
    if 'seven' == time_range:
        df['sevenDayTotalVolume'] = df['sevenDayTotalVolume'].apply(lambda x: divide_rate(x, rate))
        df = df.rename(columns={
            'collection': f'{time_range} days',
            'sevenDayTotalVolume': 'Volume',
            'sevenDayNumTotalSales': 'No of  Sales',
            'floorPrice': 'Average Floor Price',
            'numOwners': 'Owners',
        })
    else:
        df['thirtyDayTotalVolume'] = df['thirtyDayTotalVolume'].apply(lambda x: divide_rate(x, rate))
        df = df.rename(columns={
            'collection': f'{time_range} days',
            'thirtyDayTotalVolume': 'Volume',
            'thirtyDayNumTotalSales': 'No of  Sales',
            'floorPrice': 'Average Floor Price',
            'numOwners': 'Owners',
        })

    total_df = pd.concat([df, calculate_df], ignore_index=True)
    save_nifty_gateway_data_to_file(total_df, time_range)
    
    return calculate_df

def extract_values(list):
    if (list):
        new_value = f"{list[0]['max_value']} {list[0]['unit']}"
        return new_value
    else:
        return '--'

def get_nftgo_data(time_range):
    headers = {
        "accept": "application/json",
        "X-API-KEY": "13269266-950a-44f0-bd65-3c80be1c25f1"
    }    
    url = f'https://data-api.nftgo.io/eth/v1/market/rank/marketplaces/{time_range}?sort_by=volume&asc=false&offset=0&limit=10&exclude_wash_trading=false'

    result = fetch_api_data(url, headers)
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(result['marketplaces_info'])
    
    return df

def save_data_to_file(df, time_range):
    df['new_fee_rates'] = df['fee_rates'].apply(extract_values)
    # Get today's date
    today = datetime.today()

    # Get the date of 6 days ago (to make a total of 7 days including today)
    days = 6
    nifty_gateway_df = None
    if ('30d' == time_range):
        days = 29
        nifty_gateway_df = get_nifty_gateway_data('thirty')
    else:
        nifty_gateway_df = get_nifty_gateway_data('seven')
    start_date = today - timedelta(days=days)

    # Format the dates to a more readable format (e.g., 'YYYY-MM-DD')
    today_str = today.strftime('%B %d')
    start_date_str = start_date.strftime('%B %d')

    nifty_gateway_df.drop(columns=['Average Floor Price'], inplace=True)
    nifty_gateway_df['Fee Rate'] = '--'

    time_range_title = f'{time_range}({start_date_str} - {today_str})'
    df = df.rename(columns={
        'marketplace_name': time_range_title,
        'volume_eth': 'Volumes （ETH）',
        'sale_num': 'No of Sales （Sales）',
        'buyer_num': 'Owners  （Buyers）',
        'new_fee_rates': 'Fee Rate',
    })
    # columns = [time_range_title, 'Volumes （ETH）', 'No of Sales （Sales）', 'Owners  （Buyers）', 'Fee Rate']
    total_df = pd.concat([df, nifty_gateway_df], ignore_index=True)
    total_df.to_csv(f'{time_range}_data.csv', index=False)
    total_df.to_excel(f'{time_range}_data.xlsx', index=False)

time_ranges = ['7d', '30d']
for item in time_ranges:
    nftgo_data = get_nftgo_data(item)
    save_data_to_file(nftgo_data, item)