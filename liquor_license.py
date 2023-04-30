# %% [markdown]
# # Liquor License Scraping Software for IUCG X PCG Project Funnel 
# 
#     The purpose of this software is to retrieve the new liquor licenses for a state every day, or however often the data is updated. The software will then look to find existing phone numbers using google business phone number scraping to populate a phone number field in the dataset. The data will then be sent to Salesforce. There, by methods that are yet to be determined, the new data will be compared to the current database and any new businesses will be added to the database and the phone numbers and addresses will be sent down the calling and direct mailing pipelines, respectively. 
# 
# __Methods/ Tools:__
# - Chrome Driver: Tool used to automate opening links and retrieving information from state liquor website
# - Selenium Webdriver: Used to scrape pages based on unique element tag
# - Socrata: Used to retrieve data for NY state only, given they offer access via an API
# - Pandas: package used for dataframe creation and manipulation
# 
# __Data Results:__
# - Chrome Driver: Downloads and returns new business dataset from state liquor websites
# - Selenium Webdriver: Scrapes and returns phone number of business and enters into dataset
# - Socrata: Retrieves and returns new businesses for NY only
# 
# __End Product__: The data passed onto salesforce will include the fields: business name, street address, town, state, and phone number if found

# Final changes
# %%
# Basic packages needed for operating system functions and dataframe creation
import os
import pandas as pd
import requests
from collections import OrderedDict
from datetime import date
from datetime import timedelta
from datetime import datetime
# Packages for retrieving data from websites
from sodapy import Socrata
from bs4 import BeautifulSoup
import openpyxl
from lxml import html



# %%
client = Socrata("data.ny.gov", None)
date_apply = date.today() - timedelta(days = 1) 
results = client.get("t5r8-ymc5", limit=10000)
ny_liquor_df = pd.DataFrame.from_records(results)
# dt = datetime.now()
# if dt == 0:
#     date_apply = date.today() - timedelta(days=3)
ny_liquor_df['received_date'] = pd.to_datetime(ny_liquor_df['received_date']).dt.date
# filtered_ny_liquor_df = ny_liquor_df.loc[ny_liquor_df['received_date'] > date_apply]
filtered_ny_liquor_df = ny_liquor_df.loc[ny_liquor_df['received_date'] == date_apply]
filtered_ny_liquor_df['Company'] = filtered_ny_liquor_df.apply(lambda row: row['premise_name'] if pd.isna(row["premise_name2"]) else row["premise_name2"], axis=1)
i = 1
while len(filtered_ny_liquor_df) == 0:
    date_apply = date.today() - timedelta(days = i)
    filtered_ny_liquor_df = ny_liquor_df.loc[ny_liquor_df['received_date'] == date_apply]
    filtered_ny_liquor_df = filtered_ny_liquor_df.loc[~((filtered_ny_liquor_df['lic_type'] == 'HL') | (filtered_ny_liquor_df['lic_type'] =='L') | (filtered_ny_liquor_df['lic_type'] =='AX'))]
    filtered_ny_liquor_df = filtered_ny_liquor_df.drop(['comments','premise_name','nv_serial_number','lic_type','lic_class','county_name','estimated_date_of_determination','zone'], axis = 1)
    i += 1
filtered_ny_liquor_df = filtered_ny_liquor_df.loc[~((filtered_ny_liquor_df['lic_type'] == 'HL') | (filtered_ny_liquor_df['lic_type'] =='L') | (filtered_ny_liquor_df['lic_type'] =='AX'))]
filtered_ny_liquor_df = filtered_ny_liquor_df.drop(['comments','premise_name','nv_serial_number','lic_type','lic_class','county_name','estimated_date_of_determination','zone'], axis = 1)
filtered_ny_liquor_df = filtered_ny_liquor_df.rename(columns={'premise_address': 'Address1', 'premise_addesc': 'Address2', 'premise_city':'City', 'premise_state': 'State', 'premise_zip':'Zip'})
filtered_ny_liquor_df = filtered_ny_liquor_df.reindex(columns=['Company', 'Address1', 'Address2', 'City','State','Zip'])  
filtered_ny_liquor_df = filtered_ny_liquor_df.reset_index(drop = True)
filtered_ny_liquor_df

# %%
url = "http://www.myfloridalicense.com/dbpr/sto/file_download/extracts/daily.csv"
r = requests.get(url, allow_redirects=True)
request_content = r.content
csv_file = open('fldaily.csv', 'wb')
csv_file.write(request_content)
csv_file.close()
fl_liquor_df = pd.read_csv('fldaily.csv', names = ['License_Code','County','1','2','3','Location_name','Parent_name','location_address','4','5','City','State','6','Date','7','License_type','8','9'])
os.remove('fldaily.csv')
fl_liquor_df = fl_liquor_df.loc[fl_liquor_df['License_Code'] == 4006]
fl_liquor_df = fl_liquor_df[fl_liquor_df['License_type'].str.contains("Initial")|fl_liquor_df['License_type'].str.contains("Address Change")]
fl_liquor_df = fl_liquor_df.drop(['License_Code','County', '1','2','3','Parent_name','4','5','7','8','9','License_type','Date'], axis = 1)
fl_liquor_df = fl_liquor_df.reset_index(drop=True)
fl_liquor_df = fl_liquor_df.rename(columns={'Location_name': 'Company', 'location_address': 'Address1', 'state':'State', '6':'Zip'})
fl_liquor_df = fl_liquor_df.reindex(columns=['Company', 'Address1', 'Address2', 'City','State','Zip'])
fl_liquor_df['Zip'] = fl_liquor_df['Zip'].astype(int)
fl_liquor_df

# %%
client = Socrata("data.texas.gov", None)
date_apply_tx = date.today() - timedelta(days =1) 
# results_tx = client.get("7hf9-qc9f", limit=100000)
results_tx = client.get('mxm5-tdpj', limit = 2000)
texas_df = pd.DataFrame.from_records(results_tx)

texas_df['submission_date'] = pd.to_datetime(texas_df['submission_date']).dt.date
filtered_texas_df  = texas_df.loc[texas_df['submission_date'] == date_apply_tx]
i = 0
while len(filtered_texas_df) == 0:
    date_apply_tx = date.today() - timedelta(days = i)
    filtered_texas_df  = texas_df.loc[texas_df['submission_date'] == date_apply_tx]
    filtered_texas_df = filtered_texas_df.loc[((filtered_texas_df['license_type'] == 'MB') | (filtered_texas_df['license_type'] == 'FB'))]
    filtered_texas_df = filtered_texas_df.drop(['applicationid','country','license_type','applicationstatus','primary_license_id','gun_sign','master_file_id','county','wine_percent','subordinate_license_id'], axis = 1)
    i +=1
filtered_texas_df = filtered_texas_df.loc[((filtered_texas_df['license_type'] == 'MB') | (filtered_texas_df['license_type'] == 'FB'))]
filtered_texas_df = filtered_texas_df.drop(['applicationid','country','license_type','applicationstatus','primary_license_id','owner','gun_sign','master_file_id','county','wine_percent','subordinate_license_id'], axis = 1)
filtered_texas_df = filtered_texas_df.reset_index()
filtered_texas_df = filtered_texas_df.rename(columns={'trade_name': 'Company', 'address': 'Address1', 'address_2':'Address2', 'city':'City', 'state':'State', 'zip':'Zip'})

filtered_texas_df = filtered_texas_df.reindex(columns=['Company', 'Address1', 'Address2', 'City','State','Zip'])
filtered_texas_df

# %%
url = "https://www.abc.ca.gov/licensing/licensing-reports/new-applications/"
header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}
# Once you have set the url, we can now use the requests library to get the content of the url's html page.
html_page = requests.get(url)
dfs = pd.read_html(html_page.text)
ca_liquor_df = pd.DataFrame(dfs[0])
ca_liquor_df[['Type', 'Dup']] = ca_liquor_df['Type| Dup'].str.split('|', 1, expand=True)
ca_liquor_df = ca_liquor_df.drop('Type| Dup', axis = 1)
ca_liquor_df['Type'] = ca_liquor_df['Type'].astype(int)
ca_liquor_df = ca_liquor_df.loc[ca_liquor_df['Type'].isin([40, 41, 42, 47, 48, 61, 75])]
ca_liquor_df = ca_liquor_df.drop(['License Number', 'County', 'Status','Expir. Date','Action','Conditions','Escrow','District Code','Geo Code','Type','Dup'],axis=1)
ca_liquor_df['Primary Owner and Premises Addr.'] = ca_liquor_df['Primary Owner and Premises Addr.'].str.replace('DBA: ', '')
ca_liquor_df['Zip Code'] = ca_liquor_df['Zip Code'].astype(str)
ca_liquor_df['Primary Owner and Premises Addr.'] = ca_liquor_df['Primary Owner and Premises Addr.'].str.replace('\d+', ',', regex=True)
ca_liquor_df['Company'] = ca_liquor_df['Primary Owner and Premises Addr.'].str.split(',',n= 1, expand = True)[0]
ca_liquor_df['Company'] = ca_liquor_df['Company'].str.split(' ').apply(OrderedDict.fromkeys).str.join(' ')
ca_liquor_df['Company'] = ca_liquor_df['Company'].str.replace('LLC', '')
ca_liquor_df['Address1'] = ca_liquor_df['Prem Street'][~ca_liquor_df['Prem Street'].str.contains(',')]

# split the 'Name_Location' column into two columns based on the first comma delimiter only for rows that contain a comma
ca_liquor_df.loc[ca_liquor_df['Prem Street'].str.contains(','), 'Address1'] = ca_liquor_df['Prem Street'].str.split(',', n=1).str[0]
ca_liquor_df.loc[ca_liquor_df['Prem Street'].str.contains(','), 'Address2'] = ca_liquor_df['Prem Street'].str.split(',', n=1).str[1]
# ca_liquor_df['Address1', 'Address2'] = ca_liquor_df['Prem Street'].str.split(',',n= 2, expand = True)
ca_liquor_df['State'] = 'CA'
ca_liquor_df = ca_liquor_df.drop(['Mailing Addr.','Primary Owner and Premises Addr.','Prem Street','Mailing Street','Mailing City','Mailing Zip Code','Mailing State'], axis = 1)
ca_liquor_df = ca_liquor_df.rename(columns={'Zip Code': 'Zip'})
ca_liquor_df = ca_liquor_df.reindex(columns=['Company', 'Address1', 'Address2', 'City', 'State','Zip'])
ca_liquor_df = ca_liquor_df.reset_index(drop = True)
ca_liquor_df



# %%
# URL of the website
base_url = 'https://azliquor.gov/query/'
# send a GET request to the URL
response = requests.get(base_url + 'results_pendingapps.cfm')
# check if the response status code is 200
if response.status_code == 200:
    # parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    # find the input tag with the specified attributes
    input_tag = soup.find('input', {'type': 'submit', 'value': 'Export Pending to Excel', 'name': 'Excel Pending'})
    # get the form tag that contains the input tag
    form_tag = input_tag.find_parent('form')
    # extract the action attribute from the form tag
    action_url = form_tag['action']
    # construct the complete URL by appending the action URL to the base URL
    complete_url = base_url + action_url
    # extract the form data from the form tag
    form_data = {input_tag['name']: input_tag['value'], 'ExportToExcel': 'Y'}
    # send a POST request to the complete URL with the form data
    response = requests.post(complete_url, data=form_data)
    # check if the response status code is 200
    if response.status_code == 200:
        df = pd.read_html(response.content)[0]
    # create a new Excel workbook
        workbook = openpyxl.Workbook()
        # select the active worksheet
        worksheet = workbook.active
        # write the dataframe to the worksheet
        for row in df.iterrows():
            worksheet.append(row[1].tolist())
        # save the workbook to disk
        workbook.save('export.xlsx')
arizona_df = pd.read_excel('export.xlsx')
os.remove('export.xlsx')
date_apply_az = date.today() - timedelta(days = 1)
arizona_df = arizona_df.loc[arizona_df['Type'].isin(["Beer and Wine Bar", "Bar", "Restaurant"])]
arizona_df['Accepted'] = pd.to_datetime(arizona_df['Accepted']).dt.date
filtered_arizona_df  = arizona_df.loc[arizona_df['Accepted'] == date_apply_az]
i = 1
while len(filtered_arizona_df) == 0:
  date_apply_az = date.today() - timedelta(days = i)
  filtered_arizona_df = arizona_df.loc[arizona_df['Accepted'] == date_apply_az]
  i +=1
filtered_arizona_df = filtered_arizona_df.drop(['Type','County'], axis = 1)
filtered_arizona_df = filtered_arizona_df.reset_index(drop=True)
filtered_arizona_df['State'] = 'AZ'
filtered_arizona_df = filtered_arizona_df.rename(columns={'Business Name': 'Company', 'Business Address': 'Address1', 'Business Phone': 'Phone'})
filtered_arizona_df = filtered_arizona_df.reindex(columns=['Company','Address1', 'Address2','City', 'State', 'Zip','Phone', 'Licensee First Name','Licensee Last Name','Accepted'])
filtered_arizona_df['Phone'] = filtered_arizona_df['Phone'].astype(str)
filtered_arizona_df

# %%
combined_daily = pd.concat([filtered_ny_liquor_df, fl_liquor_df, filtered_texas_df, ca_liquor_df])
combined_daily = combined_daily.reset_index(drop  = True)

# %%
combined_daily

# %%
for index, row in combined_daily.iterrows():
  
    # search for the company name and the word "phone" on Google
    query = f"{row['Company'].replace(' ','+')}+{row['Address1'].replace(' ','+')}+{row['City'].replace(' ','+')}+{row['State']}+phone"
    url = f"https://www.google.com/search?q={query}"
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    
    try: 
        result = soup.find("span", class_= 'mw31Ze')
        phone_number = result.text
        print(phone_number)


    except:
        phone_number = ''


    # add the phone number to the dataframe as a new column
    combined_daily.at[index, 'Phone'] = phone_number


# %%
combined_total = pd.concat([combined_daily, filtered_arizona_df])
combined_total = combined_total.reset_index(drop = True)
combined_total.to_excel('daily_license.xlsx', index=False)


