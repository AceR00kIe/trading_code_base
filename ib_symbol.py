import pandas as pd
import time
import mysql.connector as conn

def scrap_master_symbol_list(hostname, dbname, username, password, table_name) :

    # make a inside function for scraping
    def scrap(url, tables):
        table = None # pd.DataFrame()
        try :
            table = pd.read_html(url,keep_default_na=False)[2]
        except :
            print(f"No table")
        if table == None :
            return tables, False
        else:
            tables = pd.concat([tables,table])
            return tables, True

    # Parameter for scraping symbol list from IB-website
    symbol_exchange    = ['NASDAQ','NYSE','TSEJ','SEHK']#,'IDEALPRO','SGX','NSE','ASX']
    symbol_pages       = [53,110,45,32]#,3,9,25,28]
    symbol_categories  = ['STK','STK','STK','STK']#,'','STK','STK','STK']
    symbol_type        = ['STK','STK','STK','STK']#,'CASH','STK','STK','STK']
    symbol_rm          = ['^GSPC','^GSPC','^N225','^HSI']#,'','^sti','^nsei','^axjo']
    symbol_rf          = '^IRX'

    for symbol_exchange, symbol_pages, symbol_categories, symbol_type, symbol_rm in zip(symbol_exchange,symbol_pages,symbol_categories,symbol_type,symbol_rm) :
        start_time  = time.time()
        tables      = pd.DataFrame()

        base_url    = f"https://www.interactivebrokers.com/en/index.php?f=2222&exch={symbol_exchange.lower()}&showcategories={symbol_categories}&p=&ptab=&cc=&limit=100"
        urls        = [f"{base_url}&page={symbol_pages}" for symbol_pages in range(1,symbol_pages)]

        # Multi-threading if available (faster method)
        #with pool.ThreadPool() as ex:
        #    ex.map(scrap, urls)

        for url in urls :
            tables = scrap(url, tables) # tables = table(s) concat

        # Function to convert symbol
        def convert_symbol(row):
            if row['Exchange'] == 'SEHK':
              return f"{row['Symbol']:0>4}.HK"
            else:
              return row['Symbol']

        # Configure the tables before storing in SQL
        tables['Exchange']          = symbol_exchange
        tables['Symbol']            = tables.apply(convert_symbol, axis=1) # np.where(tables.Exchange.eq('SEHK'),str(f'{int(tables.Symbol):04}.HK'), tables.Symbol )
        tables['Type']              = symbol_type
        tables                      = tables.rename(columns={'Product Description  (click link for more details)': 'Product Description'})
        tables                      = tables.sort_values(by=["Exchange","IB Symbol"])
        tables                      = tables.reset_index()
        tables                      = tables.drop(['index'], axis= 1)
        tables['rm_symbol']         = symbol_rm
        tables['rf_symbol']         = symbol_rf

        if symbol_type == 'CASH' :
            tables['ib_exchange'] = 'IDEALPRO'
        else :
            tables['ib_exchange'] = 'SMART'

        tables['Sector']         = None ## For Trigger cancel

        print(tables)
        #data = tables.values.tolist()
        data = list(tables.itertuples(index=False, name=None))
        #print(data)

        with conn.connect(host=hostname,database=dbname,user=username, passwd=password, autocommit=True) as cnx : #, use_pure=True
            cursor = cnx.cursor()
            sql = f'''
            INSERT IGNORE INTO `{table_name}` (
            `IB Symbol`, `Product Description`,`Symbol`,`Currency`,`Exchange`,`Type`,
            `rm_symbol`,`rf_symbol`,`ib_exchange`,`Sector`) VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

            ON DUPLICATE KEY UPDATE
            `IB Symbol`             = VALUES(`IB Symbol`),
            `Product Description`   = VALUES(`Product Description`),
            `Symbol`                = VALUES(`Symbol`),
            `Currency`              = VALUES(`Currency`),
            `Exchange`              = VALUES(`Exchange`),
            `Type`                  = VALUES(`Type`),
            `rm_symbol`             = VALUES(`rm_symbol`),
            `rf_symbol`             = VALUES(`rf_symbol`),
            `ib_exchange`           = VALUES(`ib_exchange`),
            `Sector`                = VALUES(`Sector`) ;
            '''
            cursor.executemany(sql, data)

        end_time = time.time()

        print(f"{symbol_exchange.upper()} completed in {round(end_time - start_time,2)} sec")
