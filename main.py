import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
from selenium import webdriver

def fetch_web_data(url):
    
    browser = webdriver.Chrome()

    browser.get(url)

    browser.implicitly_wait(10)

    page_source = browser.page_source

    browser.quit()

    return page_source

def parse_opensea_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the data - You will need to inspect the webpage to find the correct HTML elements
    # Below is a placeholder and likely needs to be adapted
    data = []
    for row in soup.find_all('a', attrs={"role": "row"}):
        row_data = [col.get_text() for col in row.find_all('div', class_='axQXd')]
        data.append(row_data)
    
    # Create a DataFrame from the scraped data
    df = pd.DataFrame(data, columns=['NO', 'Volume', '% Change', 'Floor price', 'Sales', '% Unique owners', '% Items listed']) # Replace with your column names
    df['NO'] = df['NO'].astype(int)
    df_sorted = df.sort_values(by='NO')
    return df_sorted

def fetch_api_data(url, params=None):
    """Fetch data from the API and return the JSON response."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Check if the request was successful
    return response.json()

def fetch_blur_api_data(url, params=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Referer": "https://blur.io/collections"
    }
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Check if the request was successful
    return response.json()


def get_nifty_gateway_data():
    """Get data from the Nifty Gateway API endpoint."""
    
    # Define the API endpoint and parameters
    api_endpoint = "https://api.niftygateway.com/stats/rankings/"
    params = {"page": 1, "page_size": 50, "sort": "-seven_day_total_volume"}
    
    # Fetch data from the API endpoint
    data = fetch_api_data(api_endpoint, params=params)
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data['results'])
    
    return df

def get_opensea_thirty_day_data():
    url = "https://opensea.io/rankings?sortBy=thirty_day_volume"  
    html_content = fetch_web_data(url)
    df = parse_opensea_data(html_content)
    
    return df

def get_opensea_seven_day_data():
    url = "https://opensea.io/rankings?sortBy=seven_day_volume"  
    html_content = fetch_web_data(url)
    df = parse_opensea_data(html_content)
    
    return df

def get_blur_data():
    # Define the API endpoint and parameters
    api_endpoint = "https://core-api.prod.blur.io/v1/collections/"
    params = {
        "filters": '{"sort":"VOLUME_ONE_DAY","order":"DESC"}'
    }
    data = fetch_blur_api_data(api_endpoint, params=params)
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data['collections'])
    
    return df

def save_nifty_gateway_data_to_file(df):
    """Save data to CSV and Excel files."""
    seven_day_data_columns = ['floorPrice', 'numOwners', 'sevenDayTotalVolume', 'sevenDayChange', 'sevenDaySecondaryVolume', 'totalVolume', 'avgSalePrice', 'totalPrimaryVolume', 'totalSecondaryVolume', 'sevenDayNumSecondarySales', 'sevenDayNumSecondarySalesChange', 'sevenDayNumTotalSales', 'sevenDayNumTotalSalesChange', 'totalNumTotalSales', 'totalNumSecondarySales', 'totalNumPrimarySales', 'totalMarketCap']
    thirty_day_data_columns = ['floorPrice', 'numOwners', 'thirtyDayTotalVolume', 'thirtyDayChange', 'thirtyDaySecondaryVolume', 'totalVolume', 'avgSalePrice', 'totalPrimaryVolume', 'totalSecondaryVolume', 'thirtyDayNumSecondarySales', 'thirtyDayNumSecondarySalesChange', 'thirtyDayNumTotalSales', 'thirtyDayNumTotalSalesChange', 'totalNumTotalSales', 'totalNumSecondarySales', 'totalNumPrimarySales', 'totalMarketCap']
    df.to_csv('nifty_gateway_seven_day_data.csv', columns=seven_day_data_columns, index=False)
    df.to_csv('nifty_gateway_thirty_day_data.csv', columns=seven_day_data_columns, index=False)
    df.to_excel('nifty_gateway_seven_day_data.xlsx', columns=thirty_day_data_columns, index=False)
    df.to_excel('nifty_gateway_thirty_day_data.xlsx', columns=thirty_day_data_columns, index=False)

def save_opensea_seven_data_to_file(df):
    df.to_csv('opensea_seven_day_data.csv', index=False)
    df.to_excel('opensea_seven_day_data.xlsx', index=False)

def save_opensea_thirty_data_to_file(df):
    df.to_csv('opensea_thirty_day_data.csv', index=False)
    df.to_excel('opensea_thirty_day_data.xlsx', index=False)

def save_blur_data_to_file(df):
    floorPriceOneDay = pd.json_normalize(df['floorPriceOneDay'])
    df = pd.concat([df, floorPriceOneDay], axis=1).drop(columns='floorPriceOneDay')
    print(df)
    exit()
    df.to_csv('nifty_gateway_thirty_day_data.csv', columns=seven_day_data_columns, index=False)
    df.to_excel('nifty_gateway_seven_day_data.xlsx', columns=thirty_day_data_columns, index=False)

opensea_seven_day_df = get_opensea_seven_day_data()
save_opensea_seven_data_to_file(opensea_seven_day_df)

opensea_thirty_day_df = get_opensea_thirty_day_data()
save_opensea_thirty_data_to_file(opensea_thirty_day_df)

# Get the data
nifty_gateway_df = get_nifty_gateway_data()

# Save the data to files
save_nifty_gateway_data_to_file(nifty_gateway_df)
