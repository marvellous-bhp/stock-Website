from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import time
import requests
import pandas as pd
from datetime import datetime

banks_hose = ['ACB', 'BID', 'CTG','EIB', 'HDB', 'MBB', 'MSB','OCB','SHB','SSB','STB','TCB','TPB','VCB','VIB','VPB']
banks_hnx = ['NVB', 'BAB']
data = {}



class Stock:
    def __init__(self, date, open, high, low, close, volume):
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

def get_data_from_web(bank):
    print('bank',bank)
    driver = webdriver.Chrome()
    url = f'https://simplize.vn/co-phieu/{bank}/lich-su-gia'
    driver.get(url)
    next_button = WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="phan-tich"]/div[2]/div/div/div[2]/div[1]/div/div[3]/ul/li[9]/div'))
    )
    data[bank]={}
    for i in range(1,2):    

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        tbody = soup.find('tbody', class_='simplize-table-tbody')
        tr_simplize_table_row = tbody.findAll('tr',class_='simplize-table-row')
        for tr in tr_simplize_table_row:
            h6_css_cvilom = tr.findAll('h6',class_='css-cvilom')
            volumeIndex = len(h6_css_cvilom)-1
            stockHistory={}
            date = h6_css_cvilom[0].text.strip()
            stockHistory['open'] = h6_css_cvilom[1].text.strip()
            stockHistory['high'] = h6_css_cvilom[2].text.strip()
            stockHistory['low'] = h6_css_cvilom[3].text.strip()
            stockHistory['close'] = h6_css_cvilom[4].text.strip()
            stockHistory['volume'] = h6_css_cvilom[volumeIndex].text.strip()
            data[bank][date] = str(stockHistory)
        next_button.click()
        time.sleep(2)
    print('finish {}',  bank)
    return data[bank]

def saveData(data):
    # Chuyển đổi dữ liệu thành chuỗi JSON
    json_data = json.dumps(data, indent=4)  # `indent` làm cho dữ liệu được hiển thị có cấu trúc
    # print('json',json_data)

    # Ghi dữ liệu JSON vào tệp
    with open('data_2611.json', 'w') as json_file:
        json_file.write(json_data)

def crawl_stock_data(stock_code):
    data = {}
    try:
        # URL for stock data
        url = f"https://finance.vietstock.vn/{stock_code}/tai-chinh.htm"
        
        # Headers to mimic browser request
        headers = {
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
        }
        
        # Make request to get the page
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        data[stock_code] = {}
        date = datetime.now().strftime('%d/%m/%Y')

        stockHistory = {
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'volume': None,
        }
        
        # Extract financial data
        try:
            # close_element = soup.find('span', {'class': 'price'})
            # if close_element and close_element.text.strip():
                # try:
                    # stockHistory['close'] = float(close_element.text.strip().replace(',', ''))
            stockHistory['close'] = 0
                # except ValueError:
                #     print(f"Error converting close price: {close_element.text.strip()}")
                
            open_element = soup.find('b', {'id': 'openprice'})
            if open_element and open_element.text.strip():
                try:
                    stockHistory['open'] = float(open_element.text.strip().replace(',', ''))
                except ValueError:
                    print(f"Error converting open price: {open_element.text.strip()}")
                
            volume_element = soup.find('b', {'id': 'totalvol'})
            if volume_element and volume_element.text.strip():
                try:
                    stockHistory['volume'] = int(volume_element.text.strip().replace(',', ''))
                except ValueError:
                    print(f"Error converting volume: {volume_element.text.strip()}")
                
            high_element = soup.find('b', {'id': 'highestprice'})
            if high_element and high_element.text.strip():
                try:
                    stockHistory['high'] = float(high_element.text.strip().replace(',', ''))
                except ValueError:
                    print(f"Error converting high price: {high_element.text.strip()}")
                
            low_element = soup.find('b', {'id': 'lowestprice'})
            if low_element and low_element.text.strip():
                try:
                    stockHistory['low'] = float(low_element.text.strip().replace(',', ''))
                except ValueError:
                    print(f"Error converting low price: {low_element.text.strip()}")

            # print("Raw values found:")
            # print(f"Open: {open_element.text if open_element else 'Not found'}")
            # print(f"Volume: {volume_element.text if volume_element else 'Not found'}")
            # print(f"High: {high_element.text if high_element else 'Not found'}")
            # print(f"Low: {low_element.text if low_element else 'Not found'}")
            
        except (ValueError, AttributeError) as e:
            print(f"Error parsing data: {e}")
        
        data[stock_code][date] = stockHistory
        
        return {
            'success': True,
            'data': data,
            'message': 'Data crawled successfully'
        }
        
    except requests.RequestException as e:
        return {
            'success': False,
            'data': None,
            'message': f'Error fetching data: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'data': None,
            'message': f'Unexpected error: {str(e)}'
        }