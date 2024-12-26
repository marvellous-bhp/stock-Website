import uuid
import pandas as pd
from datetime import datetime
from models import StockHistory, StockList
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

SQL_STRING = os.getenv('SQL_STRING')
banks = ['ACB', 'BID', 'CTG','EIB', 'HDB', 'MBB', 'MSB','OCB','SHB','SSB','STB','TCB','TPB','VCB','VIB','VPB','NVB', 'BAB']

# List of bank names (same as in original script)
name_symbols = [
    "Ngân hàng TMCP Á Châu", "Ngân hàng TMCP Đầu tư và Phát triển Việt Nam", 
    "Ngân hàng TMCP Công Thương Việt Nam", "Ngân hàng TMCP Xuất nhập khẩu Việt Nam",
    "Ngân hàng TMCP Phát triển TP.HCM", "Ngân hàng TMCP Quân Đội", 
    "Ngân hàng TMCP Hàng hải Việt Nam", "Ngân hàng TMCP Phương Đông",
    "Ngân hàng TMCP Sài Gòn – Hà Nội", "Ngân hàng TMCP Đông Nam Á",
    "Ngân hàng TMCP Sài Gòn Thương Tín", "Ngân hàng TMCP Kỹ thương Việt Nam",
    "Ngân hàng TMCP Tiên Phong", "Ngân hàng TMCP Ngoại Thương Việt Nam",
    "Ngân hàng TMCP Quốc tế Việt Nam", "Ngân hàng TMCP Việt Nam Thịnh Vượng",
    "Ngân hàng TMCP Quốc Dân", "Ngân hàng TMCP Bắc Á"
]

def convert_str_to_float(value):
    """Convert string or float to float."""
    if isinstance(value, str):
        return float(value.replace(',', ''))
    return float(value)  # Nếu đã là float, trả về chính nó


def convert_dict_to_df(data_dict):
    """Convert dictionary of stock data to pandas DataFrame."""
    data = {key: value for key, value in data_dict.items()}
    df = pd.DataFrame.from_dict(data, orient='index')
    df.index = pd.to_datetime(df.index, format='%d/%m/%Y')
    
    # Chỉ chuyển đổi nếu có dữ liệu kiểu chuỗi
    if not df.dtypes.apply(lambda x: pd.api.types.is_numeric_dtype(x)).all():
        df = df.applymap(convert_str_to_float)
    
    return df.sort_index(ascending=True)

def write_crawl_data_to_db(data, stock_ids):
    """
    Write crawled stock data to database using SQLAlchemy.
    
    :param db: SQLAlchemy database engine
    :param data: Dictionary of stock data
    :return: Boolean indicating successful insertion
    """
    # Create a session

    engine = create_engine(SQL_STRING)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Validate input data
        if not data:
            print("No data to insert")
            return False
        
        # Prepare stock list data
        symbols = banks
        
        # Ensure we have enough bank names
        if len(symbols) > len(name_symbols):
            name_symbols.extend([f"Unknown Bank {i+1}" for i in range(len(symbols) - len(name_symbols))])
        
        stocks = [list(data[symbol].values()) for symbol in symbols]
        previous_close_prices = []
        
        for stock in stocks:
            try:
                previous_close_price = stock[0]["close"]
                previous_close_prices.append(str(previous_close_price))
            except (IndexError, KeyError) as e:
                print(f"Error extracting close price: {e}")
                previous_close_prices.append('0')  # Default value
                
        # Prepare stock list insertions
        stock_list_data = [
            {
                'stockid': stock_ids[i], 
                'symbol': symbols[i], 
                'company_name': name_symbols[i], 
                'previous_close_price': float(str(previous_close_prices[i]).replace(',', '') or '0')
            } 
            for i in range(len(stocks))
        ]
        
        # Debug print
        print(f"Preparing to insert {len(stock_list_data)} stocks")
        
        # Bulk insert stock list data using Insert statement
        try:
            insert_stmt = insert(StockList).values(stock_list_data)
            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                symbol=insert_stmt.inserted.symbol,
                company_name=insert_stmt.inserted.company_name,
                previous_close_price=insert_stmt.inserted.previous_close_price
            )
            session.execute(on_duplicate_key_stmt)
        except Exception as insert_error:
            print(f"Error during stock list insertion: {insert_error}")
            raise
        
        # Prepare and insert stock history data
        for symbol, stockid in zip(symbols, stock_ids):
            try:
                data_history = data[symbol]
                data_history_df = convert_dict_to_df(data_history)
                
                stock_history_data = []
                for index, row in data_history_df.iterrows():
                    stock_history_data.append({
                        'stockid': stockid,
                        'date': index.date(),
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': int(row['volume'])
                    })
                
                # Bulk insert stock history data
                session.bulk_insert_mappings(StockHistory, stock_history_data)
            
            except Exception as history_error:
                print(f"Error inserting history for {symbol}: {history_error}")
                # Continue with next symbol instead of failing entire process
                continue
        
        # Commit the transaction
        session.commit()
        print(f"Successfully inserted data for {len(symbols)} stocks")
        return True
    
    except Exception as e:
        # Rollback in case of error
        session.rollback()
        print(f"An error occurred during database write: {e}")
        import traceback
        traceback.print_exc()  # Print full traceback for debugging
        return False
    
    finally:
        # Close the session
        session.close()