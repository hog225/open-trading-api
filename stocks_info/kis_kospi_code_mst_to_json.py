'''코스피주식종목코드(kospi_code.mst)를 JSON으로 정제하는 파이썬 파일'''

import urllib.request
import ssl
import zipfile
import os
import pandas as pd

base_dir = os.getcwd()

def kospi_master_download(base_dir, verbose=False):
    cwd = os.getcwd()
    if (verbose): print(f"current directory is {cwd}")
    ssl._create_default_https_context = ssl._create_unverified_context

    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
                               os.path.join(base_dir, "kospi_code.zip"))

    os.chdir(base_dir)
    if (verbose): print(f"change directory to {base_dir}")
    kospi_zip = zipfile.ZipFile('kospi_code.zip')
    kospi_zip.extractall()

    kospi_zip.close()

    if os.path.exists("kospi_code.zip"):
        os.remove("kospi_code.zip")


def get_kospi_master_dataframe(base_dir):
    file_name = os.path.join(base_dir, "kospi_code.mst")
    tmp_fil1 = os.path.join(base_dir, "kospi_code_part1.tmp")
    tmp_fil2 = os.path.join(base_dir, "kospi_code_part2.tmp")

    wf1 = open(tmp_fil1, mode="w", encoding="cp949")
    wf2 = open(tmp_fil2, mode="w", encoding="cp949")

    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0:len(row) - 228]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
            rf2 = row[-228:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    part1_columns = ['단축코드', '표준코드', '한글명']
    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='cp949')

    # Since we only need columns from the first part, we can simplify
    # and not read the second part.
    os.remove(tmp_fil1)
    os.remove(tmp_fil2)
    
    return df1


# Main execution block
kospi_master_download(base_dir)
df = get_kospi_master_dataframe(base_dir) 

# 1. Filter for required columns
# We only need '단축코드' and '한글명' which are already in df
df_filtered = df[['단축코드', '한글명']].copy()

# 2. Rename columns to the desired JSON keys
df_filtered.rename(columns={'단축코드': 'symbol', '한글명': 'name'}, inplace=True)

# 3. Add new static columns with fixed values
df_filtered['nationCode'] = 'KR'
df_filtered['exchangeName'] = 'KOSPI'
df_filtered['currency'] = 'KRW'

# 4. Save the final dataframe to a JSON file
output_filename = 'kospi_code.json'
df_filtered.to_json(output_filename, orient='records', indent=4, force_ascii=False)

print(f"'{output_filename}' file created successfully.")

