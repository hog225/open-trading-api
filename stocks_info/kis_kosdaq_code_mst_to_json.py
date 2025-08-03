'''코스닥주식종목코드(kosdaq_code.mst)를 JSON으로 정제하는 파이썬 파일'''

import pandas as pd
import urllib.request
import ssl
import zipfile
import os

base_dir = os.getcwd()

def kosdaq_master_download(base_dir, verbose=False):
    cwd = os.getcwd()
    if (verbose): print(f"current directory is {cwd}")
    ssl._create_default_https_context = ssl._create_unverified_context
    
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
                               os.path.join(base_dir, "kosdaq_code.zip"))

    os.chdir(base_dir)
    if (verbose): print(f"change directory to {base_dir}")
    kosdaq_zip = zipfile.ZipFile('kosdaq_code.zip')
    kosdaq_zip.extractall()
    
    kosdaq_zip.close()

    if os.path.exists("kosdaq_code.zip"):
        os.remove("kosdaq_code.zip")

def get_kosdaq_master_dataframe(base_dir):
    file_name = os.path.join(base_dir, "kosdaq_code.mst")
    tmp_fil1 = os.path.join(base_dir, "kosdaq_code_part1.tmp")
    tmp_fil2 = os.path.join(base_dir, "kosdaq_code_part2.tmp")

    wf1 = open(tmp_fil1, mode="w", encoding="cp949")
    wf2 = open(tmp_fil2, mode="w", encoding="cp949")

    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0:len(row) - 222]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
            rf2 = row[-222:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    part1_columns = ['단축코드','표준코드','한글종목명']
    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='cp949')

    # Clean up temporary files as we only need the first part
    os.remove(tmp_fil1)
    os.remove(tmp_fil2)

    return df1

# Main execution block
kosdaq_master_download(base_dir)
df = get_kosdaq_master_dataframe(base_dir)

# 1. Filter for required columns
df_filtered = df[['단축코드', '한글종목명']].copy()

# 2. Rename columns to the desired JSON keys
df_filtered.rename(columns={'단축코드': 'symbol', '한글종목명': 'name'}, inplace=True)

# 3. Add new static columns with fixed values
df_filtered['nationCode'] = 'KR'
df_filtered['exchangeName'] = 'KOSDAQ'
df_filtered['currency'] = 'KRW'

# 4. Save the final dataframe to a JSON file
output_filename = 'kosdaq_code.json'
df_filtered.to_json(output_filename, orient='records', indent=4, force_ascii=False)

print(f"'{output_filename}' file created successfully.")