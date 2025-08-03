'''해외주식종목코드 정제 파이썬 파일
미국 : nasmst.cod, nysmst.cod, amsmst.cod, 
중국 : shsmst.cod, shimst.cod, szsmst.cod, szimst.cod, 
일본 : tsemst.cod, 
홍콩 : hksmst.cod, 
베트남 : hnxmst.cod, hsxmst.cod'''

'''
※ 유의사항 ※
실행 환경 혹은 원본 파일의 칼럼 수의 변경으로 간혹 정제코드 파일(overseas_stock_code.py)이 실행되지 않을 수 있습니다.
해당 경우, URL에 아래 링크를 복사+붙여넣기 하여 원본 파일을 다운로드하시기 바랍니다.
. https://new.real.download.dws.co.kr/common/master/{val}mst.cod.zip
. {val} 자리에 원하시는 시장코드를 넣어주세요.
. 'nas','nys','ams','shs','shi','szs','szi','tse','hks','hnx','hsx'
. 순서대로 나스닥, 뉴욕, 아멕스, 상해, 상해지수, 심천, 심천지수, 도쿄, 홍콩, 하노이, 호치민
'''

import pandas as pd
import urllib.request
import ssl
import zipfile
import os

base_dir = os.getcwd()

def get_overseas_master_dataframe(base_dir,val):

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve(f"https://new.real.download.dws.co.kr/common/master/{val}mst.cod.zip", os.path.join(base_dir, f"{val}mst.cod.zip"))
    os.chdir(base_dir)

    overseas_zip = zipfile.ZipFile(f'{val}mst.cod.zip')
    overseas_zip.extractall()
    overseas_zip.close()

    file_name = os.path.join(base_dir, f"{val}mst.cod")
    columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time(HHMM)', 'market end time(HHMM)', 'DR 여부(Y/N)', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부(0:구성종목없음,1:구성종목있음)', 'Tick size Type', '구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)','Tick size type 상세']
    
    print(f"Downloading...{val}mst.cod")
    try:
        df = pd.read_table(os.path.join(base_dir, f"{val}mst.cod"), sep='\t',encoding='cp949', on_bad_lines='skip', header=None)
        df.columns = columns
        
        selected_columns = ['National code', 'Exchange name', 'Symbol', 'Korea name', 'English name', 'currency']
        df_filtered = df[selected_columns].copy()

        # Rename columns to remove spaces for JSON keys
        df_filtered.columns = ['nationCode', 'exchangeName', 'symbol', 'korName', 'name', 'currency']

        df_filtered.to_json(f'{val}_code.json', orient='records', indent=4, force_ascii=False)

    except Exception as e:
        print(f"Error processing file {val}mst.cod: {e}")
        return None
    
    return df_filtered

cmd = input("1:전부 다운로드, 2:1개의 시장을 다운로드 \n")

if cmd =='1':
    lst = ['nas','nys','ams','shs','shi','szs','szi','tse','hks','hnx','hsx'] 

    DF=pd.DataFrame()
    for i in lst:
        temp = get_overseas_master_dataframe(base_dir,i)
        if temp is not None:
            DF = pd.concat([DF,temp],axis=0)
            
    if not DF.empty:
        print("Saving...overseas_stock_code(all).json")
        DF.to_json('overseas_stock_code(all).json', orient='records', indent=4, force_ascii=False)
    print("Done")

elif cmd =='2':
    while True:
        cmd2 = input("다운로드하시고자 하는 시장의 코드를 입력하여 주세요. \nnas:나스닥, nys:뉴욕, ams:아멕스, shs:상해, shi:상해지수, szs:심천, szi:심천지수, tse:도쿄, hks:홍콩, hnx:하노이, hsx:호치민\n")
        df = get_overseas_master_dataframe(base_dir,cmd2)
        if df is not None:
            print("Done")
            break;