import requests
from io import BytesIO
import pandas as pd
import re

maximum_loop = 1
year = '2023'
report_type = '사업보고서'

def download_fs(url, company_name):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67'
    resp = requests.get(url, headers = {"user-agent": user_agent})
    table = BytesIO(resp.content)
    pocket = ['재무상태표', '손익계산서', '포괄손익계산서']

    for sheet in pocket:
        data = pd.read_excel(table, sheet_name=sheet, skiprows=6)
        data.to_csv(company_name + '_' + sheet + ".csv", encoding="euc-kr")

def get_rcp_dcm_code(corp_code):
    url = 'https://opendart.fss.or.kr/api/list.xml?crtfc_key=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx&corp_code={}&bgn_de={}0101&end_de={}1231&pblntf_ty=A&pblntf_detail_ty=A001&last_reprt_at=Y&page_no=1&page_count=10'.format(corp_code, year, year)
    print('RCP URL : {}'.format(url))
    resp = requests.get(url)
    webpage = resp.content.decode('UTF-8')
    rcp_no_list = re.findall(r'<rcept_no>(.*?)</rcept_no>', webpage)
    report_nm_list = re.findall(r'<report_nm>(.*?)</report_nm>', webpage)

    dcm_no_list = []
    for rcp_no in rcp_no_list:
        resp = requests.get('http://dart.fss.or.kr/dsaf001/main.do?rcpNo={}'.format(rcp_no))
        webpage = resp.content.decode('UTF-8')
        dcm_no = re.findall(r"{}', '(.*?)',".format(rcp_no), webpage)[0]
        dcm_no_list.append(dcm_no)

    url_list = []
    rcp_no = 0
    dcm_no = 0
    for url in zip(rcp_no_list, dcm_no_list, report_nm_list):
        #print('name = {}, rcp_no = {}, dcm_no = {}'.format(url[2], url[0], url[1]))
        if(url[2].find(report_type)>=0):
            rcp_no = int(url[0])
            dcm_no = int(url[1])
    return rcp_no, dcm_no

def get_fs(rcp_no, dcm_no):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67'
    s_url = 'http://dart.fss.or.kr/pdf/download/excel.do?rcp_no={}&dcm_no={}&lang=ko'.format(rcp_no, dcm_no)
    print('FS URL : {}'.format(s_url))
    resp = requests.get(s_url, headers = {"user-agent": user_agent})
    table = BytesIO(resp.content)

    try:
        fs_data = pd.read_excel(table, sheet_name='손익계산서', names=['a', 'b', 'c', 'd'], skiprows=6, na_values='')
    except:
        print("FS Exception")
        fs_data = pd.read_excel(table, sheet_name='포괄손익계산서', name=['a', 'b', 'c', 'd'], skiprows=6, na_values='')
        return '', 0
    excel_position = 0
    
    for i in range (0, len(fs_data['a'].values.tolist())):
        if(str(fs_data['a'][i]).find('영업이익')>=0):
            excel_position = i
            break
    if(excel_position != 0):
        amount1 = fs_data['b'][excel_position]
    else:
        amount1 = 0

    excel_position = 0
    for i in range (0, len(fs_data['a'].values.tolist())):
        if(str(fs_data['a'][i]).find('당기순')>=0):
            excel_position = i
            break
    if(excel_position != 0):
        amount2 = fs_data['b'][excel_position]
    else:
        amount2 = 0
    return amount1, amount2

def main_func():
    df = pd.read_excel('종목코드.xlsx', sheet_name='종목코드')
    data = df.fillna('')

    data_list = df['COMP_CODE'].values.tolist()
    loop_count = 0
    for i in range(0, len(data_list)):
        if(data['RCP_NO'][i] != ''):
            s_code = data['COMP_CODE'][i]
            s_name = data['COMP_NAME'][i]
            print('loop = {}, count = {}/{}, code = {}, name = {}'.format(loop_count, str(i), len(data_list), str(s_code).zfill(6), s_name))
            
            print('get_rcp_dcm_code')
            result_code = get_rcp_dcm_code(str(s_code).zfill(6))
            comp_code = str(s_code).zfill(6)
            rcp_no = result_code[0]
            dcm_no = result_code[1]

            print('get_fs')
            result_account = get_fs(rcp_no, dcm_no)
            ebit = result_account[0]
            retain_earning = result_account[1]

            data['COMP_CODE'][i] = comp_code
            data['RCP_NO'][i] = rcp_no
            data['DCM_NO'][i] = dcm_no
            data['EBIT'][i] = ebit
            data['RE'][i] = retain_earning
            data.to_excel('종목코드.xlsx', sheet_name='종목코드', index=False)
            loop_count = loop_count + 1
        if(loop_count > maximum_loop):
            break
    data.to_excel('종목코드_create.xlsx', sheet_name='종목코드', index=False)

print("Start")
main_func()
print("End")
