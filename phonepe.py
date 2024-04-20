#Import Packages 

import git
import pandas as pd
import json
import os
import mysql.connector
import streamlit as st
import numpy as np
import plotly as plt
import plotly.express as px

#SQL Connection

mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="phonepe"
)

cursor = mydb.cursor()

#Clone Datas from Github

'''repository_url="https://github.com/PhonePe/pulse"
destination_directory="C:/Users/HP/Desktop/Data Scientist/my project/phonepe"
git.Repo.clone_from(repository_url,destination_directory)'''

#Created and stored Aggrecated Transaction path in a variable
path_agg_transaction=r"C:/Users/HP/Desktop/Data Scientist/my project/phonepe/data/aggregated/transaction/country/india/state/"
Agg_tran_state_list=os.listdir(path_agg_transaction)

def agg_tran(Agg_tran_state_list):
    data_list={'State':[],'Year':[],'Quater':[],'Transaction_Type':[],'Transaction_Count':[],'Transaction_Amount':[]}

    for i in Agg_tran_state_list:
        i_y=path_agg_transaction+i+"/"
        agg_year=os.listdir(i_y)
        for j in agg_year:
            j_y=i_y+j+"/"
            agg_year_list=os.listdir(j_y)
            for k in agg_year_list:
                k_jf=j_y+k
                data=open(k_jf,'r')
                load=json.load(data)
                for L in load['data']['transactionData']:
                    Name=L['name']
                    Count=L['paymentInstruments'][0]['count']
                    Amount=L['paymentInstruments'][0]['amount']
                    data_list['Transaction_Type'].append(Name)
                    data_list['Transaction_Count'].append(Count)
                    data_list['Transaction_Amount'].append(Amount)
                    data_list['State'].append(i)
                    data_list['Year'].append(j)
                    data_list['Quater'].append(int(k.strip('.json')))

    cursor.execute('''CREATE TABLE IF NOT EXISTS agg_trasaction(state varchar(255),
                                                                year int,
                                                                quater int,
                                                                name varchar(300),
                                                                count bigint,
                                                                amount bigint) ''')
    for i in range(len(data_list['State'])):
        state = data_list['State'][i]
        year = data_list['Year'][i]
        quarter = data_list['Quater'][i]
        name = data_list['Transaction_Type'][i]
        count = data_list['Transaction_Count'][i]
        amount = data_list['Transaction_Amount'][i]
        
        cursor.execute('''INSERT INTO agg_trasaction(state, year, quater, name, count, amount)
                        VALUES(%s, %s, %s, %s, %s, %s)''',
                    (state, year, quarter, name, count, amount))

    mydb.commit()

    agg_transaction=pd.DataFrame(data_list)        


#Created and stored Aggrecated User path in a variable
path_agg_user=r"C:/Users/HP/Desktop/Data Scientist/my project/phonepe/data/aggregated/user/country/india/state/"
agg_user_state_list=os.listdir(path_agg_user)


def path_user(agg_user_state_list):
    #Create a empty list for store a fetched data
    data_list1 = {'State':[], 'Year':[], 'Quater':[], 'Registered_User':[], 'App_Opens':[], 'User_Brand':[], 'User_Count':[], 'User_Percentage':[]}

    for i_u in agg_user_state_list:
        i_yu = os.path.join(path_agg_user, i_u)
        user_year = os.listdir(i_yu)
        for j_u in user_year:
            j_yu = os.path.join(i_yu, j_u)
            user_year_list = os.listdir(j_yu)
            for k_u in user_year_list:
                k_jfu = os.path.join(j_yu, k_u)
                with open(k_jfu, 'r') as file:
                    load1 = json.load(file)

                    aggregated_data = load1['data']['aggregated']
                    Registered_User = aggregated_data['registeredUsers']
                    Apps_Opens = aggregated_data['appOpens']

                    if load1['data'].get('usersByDevice'):
                        for device_data in load1['data']['usersByDevice']:
                            User_Brand = device_data['brand']
                            User_Count = device_data['count']
                            User_Percentage = device_data['percentage']

                            data_list1['Registered_User'].append(Registered_User)
                            data_list1['App_Opens'].append(Apps_Opens)
                            data_list1['User_Brand'].append(User_Brand)
                            data_list1['User_Count'].append(User_Count)
                            data_list1['User_Percentage'].append(User_Percentage)
                            data_list1['State'].append(i_u)
                            data_list1['Year'].append(j_u)
                            data_list1['Quater'].append(int(k_u.strip('.json')))

    cursor.execute('''CREATE TABLE IF NOT EXISTS agg_user(state varchar(200),
                                                        year int,
                                                        quater int,
                                                        registered_user bigint,
                                                        apps_open bigint,
                                                        user_brand varchar(200),
                                                        user_count bigint,
                                                        user_percentage float)  ''')

    for i in range(len(data_list1['State'])):
        state = data_list1['State'][i]
        year = data_list1['Year'][i]
        quarter = data_list1['Quater'][i]
        registered_user = data_list1['Registered_User'][i] if i < len(data_list1['Registered_User']) else None
        apps_open = data_list1['App_Opens'][i] if i < len(data_list1['App_Opens']) else None
        user_brand = data_list1['User_Brand'][i] if i < len(data_list1['User_Brand']) else None
        user_count = data_list1['User_Count'][i] if i < len(data_list1['User_Count']) else None
        user_percentage = data_list1['User_Percentage'][i] if i < len(data_list1['User_Percentage']) else None
        
    

        cursor.execute('''INSERT INTO agg_user(state, year, quater, registered_user, apps_open, user_brand,user_count,user_percentage)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s)''',
                    (state, year, quarter, registered_user, apps_open, user_brand,user_count,user_percentage))

    mydb.commit()

    agg_user = pd.DataFrame(data_list1)



#Created and stored Map Transaction path in a variable
path_map_transaction=r"C:/Users/HP/Desktop/Data Scientist/my project/phonepe/data/map/transaction/hover/country/india/state/"
map_trans_stat_list=os.listdir(path_map_transaction)

def map_trans(map_trans_stat_list):
    data_list2={'State':[],'Year':[],'Quater':[],'NAME':[],'Transaction_Count':[],'Transaction_Amount':[]}

    for i_m in map_trans_stat_list:
        i_my=path_map_transaction+i_m+"/"
        map_year=os.listdir(i_my)
        for j_m in map_year:
            j_my=i_my+j_m+"/"
            map_year_list=os.listdir(j_my)
            for k_m in map_year_list:
                k_mjf=j_my+k_m
                data2=open(k_mjf,'r')
                load2=json.load(data2)

                for L_m in load2['data']['hoverDataList']:
                    Name=L_m['name']
                    Count=L_m['metric'][0]['count']
                    Amount=L_m['metric'][0]['amount']
                    
                    data_list2['NAME'].append(Name)
                    data_list2['Transaction_Count'].append(Count)
                    data_list2['Transaction_Amount'].append(Amount)
                    data_list2['State'].append(i_m)
                    data_list2['Year'].append(j_m)
                    data_list2['Quater'].append(int(k_m.strip('.json')))

    cursor.execute('''CREATE TABLE IF NOT EXISTS map_transaction(state varchar(300),
                                                                year int,
                                                                quater int,
                                                                name varchar(200),
                                                                count bigint,
                                                                amount bigint) ''')

    for i in range(len(data_list2['State'])):
        state = data_list2['State'][i]
        year = data_list2['Year'][i]
        quarter = data_list2['Quater'][i]
        name = data_list2['NAME'][i]
        count = data_list2['Transaction_Count'][i]
        amount = data_list2['Transaction_Amount'][i]
        
        cursor.execute('''INSERT INTO map_transaction(state, year, quater, name, count, amount)
                        VALUES(%s, %s, %s, %s, %s, %s)''',
                    (state, year, quarter, name, count, amount))

    mydb.commit()

    map_transaction=pd.DataFrame(data_list2)


#Created and stored Map User path in a variable
path_map_user=r"C:/Users/HP/Desktop/Data Scientist/my project/phonepe/data/map/user/hover/country/india/state"
map_user_state_list=os.listdir(path_map_user)


def map_user(map_user_state_list):
    #Create a empty list for store a fetched data
    data_list3={'State':[], 'Year':[], 'Quater':[], 'Registered_User':[], 'App_Opens':[]}

    for i_mu in map_user_state_list:
        i_myu = os.path.join(path_map_user, i_mu)
        map_user_year = os.listdir(i_myu)
        for j_mu in map_user_year:
            j_myu = os.path.join(i_myu, j_mu)
            map_user_year_list = os.listdir(j_myu)
            for k_mu in map_user_year_list:
                k_mjfu = os.path.join(j_myu, k_mu)
                with open(k_mjfu, 'r') as file:
                    load3 = json.load(file)

                    hover_data = load3['data']['hoverData']
                    
                    for state, state_data in hover_data.items():
                        Registered_User = state_data.get('registeredUsers', None)
                        Apps_Opens = state_data.get('appOpens', None)

                        data_list3['Registered_User'].append(Registered_User)
                        data_list3['App_Opens'].append(Apps_Opens)
                        data_list3['State'].append(state)
                        data_list3['Year'].append(j_mu)
                        data_list3['Quater'].append(int(k_mu.strip('.json')))

    cursor.execute('''CREATE TABLE IF NOT EXISTS map_user(state varchar(300),
                                                        year int,
                                                        quater int,
                                                        registered_user bigint,
                                                        apps_open bigint  ) ''')

    for i in range(len(data_list3['State'])):
        state = data_list3['State'][i]
        year = data_list3['Year'][i]
        quarter = data_list3['Quater'][i]
        registered_user = data_list3['Registered_User'][i] if i < len(data_list3['Registered_User']) else None
        apps_open = data_list3['App_Opens'][i] if i < len(data_list3['App_Opens']) else None

        cursor.execute('''INSERT INTO map_user(state, year, quater, registered_user, apps_open)
                        VALUES(%s, %s, %s, %s, %s)''',
                    (state, year, quarter, registered_user, apps_open))
        
    mydb.commit()
        
    map_user = pd.DataFrame(data_list3)


#Created and stored Top Transaction path in a variable
path_top_transaction=r"C:/Users/HP/Desktop/Data Scientist/my project/phonepe/data/top/transaction/country/india/state/"
top_transac_state_list=os.listdir(path_top_transaction)

def top_trans(top_transac_state_list):
    # Initialize empty lists to store data for states, districts, and pincodes
    data_list_state = {'State': [], 'Year': [], 'Quater': [], 'States':[], 'Transaction_Count': [], 'Transaction_Amount': []}
    data_list_districts = {'State': [], 'Year': [], 'Quater': [], 'Districts':[] , 'Transaction_Count': [], 'Transaction_Amount': []}
    data_list_pincode = {'State': [], 'Year': [], 'Quater': [], 'pincodes':[],  'Transaction_Count': [], 'Transaction_Amount': []}

    # Iterate over each state file
    for i_tt in top_transac_state_list:
        i_ty = os.path.join(path_top_transaction, i_tt)
        top_year = os.listdir(i_ty)
        
        # Iterate over each year directory
        for j_t in top_year:
            j_ty = os.path.join(i_ty, j_t)
            top_year_list = os.listdir(j_ty)
            
            # Iterate over each quarter file
            for k_t in top_year_list:
                k_tjf = os.path.join(j_ty, k_t)
                
                # Load JSON data from file
                with open(k_tjf, 'r') as file:
                    load4 = json.load(file)

                    # Process data for states if available
                    states_data = load4['data'].get('states')
                    if states_data is not None:
                        for state in states_data:
                            state_name = state.get('entityName')
                            data_list_state['State'].append(i_tt)
                            data_list_state['Year'].append(j_t)
                            data_list_state['Quater'].append(int(k_t.strip('.json')))
                            data_list_state['States'].append(state_name if state_name is not None else 'Unknown State')
                            data_list_state['Transaction_Count'].append(state['metric'].get('count', None))
                            data_list_state['Transaction_Amount'].append(state['metric'].get('amount', None))

                    # Process data for districts
                    districts_data = load4['data'].get('districts')
                    if districts_data is not None:
                        for district in districts_data:
                            district_name = district.get('entityName')
                            data_list_districts['State'].append(i_tt)
                            data_list_districts['Year'].append(j_t)
                            data_list_districts['Quater'].append(int(k_t.strip('.json')))
                            data_list_districts['Districts'].append(district_name if district_name is not None else 'Unknown District')
                            data_list_districts['Transaction_Count'].append(district['metric'].get('count', None))
                            data_list_districts['Transaction_Amount'].append(district['metric'].get('amount', None))

                    # Process data for pincodes
                    pincodes_data = load4['data'].get('pincodes')
                    if pincodes_data is not None:
                        for pincode in pincodes_data:
                            pincode_name = pincode.get('entityName')
                            data_list_pincode['State'].append(i_tt)
                            data_list_pincode['Year'].append(j_t)
                            data_list_pincode['Quater'].append(int(k_t.strip('.json')))
                            data_list_pincode['pincodes'].append(pincode_name if pincode_name is not None else 'Unknown Pincode')
                            data_list_pincode['Transaction_Count'].append(pincode['metric'].get('count', None))
                            data_list_pincode['Transaction_Amount'].append(pincode['metric'].get('amount', None))

    cursor.execute('''CREATE TABLE IF NOT EXISTS top_tran_state(states varchar(200),
                                                                year int,
                                                                quater int,
                                                                state varchar(200),
                                                                count bigint,
                                                                amount bigint ) ''')

    for i in range(len(data_list_state['State'])):
        state = data_list_state['State'][i]
        year = data_list_state['Year'][i]
        quarter = data_list_state['Quater'][i]
        states = data_list_state['States'][i]
        count = data_list_state['Transaction_Count'][i]
        amount = data_list_state['Transaction_Amount'][i]
        
        cursor.execute('''INSERT INTO top_tran_state(states, year, quater, state, count, amount)
                        VALUES(%s, %s, %s, %s, %s, %s)''',
                    (state, year, quarter, states, count, amount))

    cursor.execute('''CREATE TABLE IF NOT EXISTS top_tran_districts(states varchar(200),
                                                                year int,
                                                                quater int,
                                                                district varchar(200),
                                                                count bigint,
                                                                amount bigint ) ''')

    for i in range(len(data_list_districts['State'])):
        state = data_list_districts['State'][i]
        year = data_list_districts['Year'][i]
        quarter = data_list_districts['Quater'][i]
        districts = data_list_districts['Districts'][i]
        count = data_list_districts['Transaction_Count'][i]
        amount = data_list_districts['Transaction_Amount'][i]
        
        cursor.execute('''INSERT INTO top_tran_districts(states, year, quater, district, count, amount)
                        VALUES(%s, %s, %s, %s, %s, %s)''',
                    (state, year, quarter, districts, count, amount))

    cursor.execute('''CREATE TABLE IF NOT EXISTS top_tran_pincode(states varchar(200),
                                                                year int,
                                                                quater int,
                                                                pincode varchar(200),
                                                                count bigint,
                                                                amount bigint ) ''')

    for i in range(len(data_list_pincode['State'])):
        state = data_list_pincode['State'][i]
        year = data_list_pincode['Year'][i]
        quarter = data_list_pincode['Quater'][i]
        pincodes = data_list_pincode['pincodes'][i]
        count = data_list_pincode['Transaction_Count'][i]
        amount = data_list_pincode['Transaction_Amount'][i]
        
        cursor.execute('''INSERT INTO top_tran_pincode(states, year, quater, pincode, count, amount)
                        VALUES(%s, %s, %s, %s, %s, %s)''',
                    (state, year, quarter, pincodes, count, amount))

    mydb.commit()

    # Create DataFrames from the collected data
    top_transaction_state = pd.DataFrame(data_list_state)
    top_transaction_districts = pd.DataFrame(data_list_districts)
    top_transaction_pincode = pd.DataFrame(data_list_pincode)



path_top_user="C:/Users/HP/Desktop/Data Scientist/my project/phonepe/data/top/user/country/india/state/"
top_user_state_list=os.listdir(path_top_user)

def top_user(top_user_state_list):
    # Initialize empty lists to store data for states, districts, and pincodes
    data_list_state1 = {'State': [], 'Year': [], 'Quater': [], 'Name':[],'Registered_User':[]}
    data_list_districts1 = {'State': [], 'Year': [], 'Quater': [], 'Name':[],'Registered_User':[]}
    data_list_pincode1 = {'State': [], 'Year': [], 'Quater': [], 'Name':[],'Registered_User':[]}

    # Iterate over each state file
    for i_ttu in top_user_state_list:
        i_tyu = os.path.join(path_top_user, i_ttu)
        top_year_user = os.listdir(i_tyu)
        
        # Iterate over each year directory
        for j_tu in top_year_user:
            j_tyu = os.path.join(i_tyu, j_tu)
            top_year_user_list = os.listdir(j_tyu)
            
            # Iterate over each quarter file
            for k_tu in top_year_user_list:
                k_tjfu = os.path.join(j_tyu, k_tu)
                
                # Load JSON data from file
                with open(k_tjfu, 'r') as file:
                    load5 = json.load(file)

                    # Process data for states if available
                    states_data = load5['data'].get('states')
                    if states_data is not None:
                        for state in states_data:
                            data_list_state1['State'].append(i_ttu)
                            data_list_state1['Year'].append(j_tu)
                            data_list_state1['Quater'].append(int(k_tu.strip('.json')))
                            data_list_state1['Name'].append(state.get('name', None))
                            data_list_state1['Registered_User'].append(state.get('registeredUsers', None))

                    # Process data for districts
                    districts_data = load5['data'].get('districts')
                    if districts_data is not None:
                        for district in districts_data:
                            data_list_districts1['State'].append(i_ttu)
                            data_list_districts1['Year'].append(j_tu)
                            data_list_districts1['Quater'].append(int(k_tu.strip('.json')))
                            data_list_districts1['Name'].append(district.get('name',None))
                            data_list_districts1['Registered_User'].append(district.get('registeredUsers', None))

                    # Process data for pincodes
                    pincodes_data = load5['data'].get('pincodes')
                    if pincodes_data is not None:
                        for pincode in pincodes_data:
                            data_list_pincode1['State'].append(i_ttu)
                            data_list_pincode1['Year'].append(j_tu)
                            data_list_pincode1['Quater'].append(int(k_tu.strip('.json')))
                            data_list_pincode1['Name'].append(pincode.get('name', None))
                            data_list_pincode1['Registered_User'].append(pincode.get('registeredUsers', None))

    cursor.execute('''CREATE TABLE IF NOT EXISTS top_user_state(states varchar(200),
                                                                year int,
                                                                quater int,
                                                                name varchar(200),
                                                                registered_user bigint ) ''')

    for i in range(len(data_list_state1['State'])):
        state = data_list_state1['State'][i]
        year = data_list_state1['Year'][i]
        quarter = data_list_state1['Quater'][i]
        names = data_list_state1['Name'][i]
        rgister_user = data_list_state1['Registered_User'][i]
        
        cursor.execute('''INSERT INTO top_user_state(states, year, quater, name, registered_user)
                        VALUES(%s, %s, %s, %s, %s)''',
                    (state, year, quarter, names, rgister_user))

    cursor.execute('''CREATE TABLE IF NOT EXISTS top_user_districts(states varchar(200),
                                                                year int,
                                                                quater int,
                                                                name varchar(200),
                                                                registered_user bigint ) ''')

    for i in range(len(data_list_districts1['State'])):
        state = data_list_districts1['State'][i]
        year = data_list_districts1['Year'][i]
        quarter = data_list_districts1['Quater'][i]
        names = data_list_districts1['Name'][i]
        rgister_user = data_list_districts1['Registered_User'][i]
        
        cursor.execute('''INSERT INTO top_user_districts(states, year, quater, name, registered_user)
                        VALUES(%s, %s, %s, %s, %s)''',
                    (state, year, quarter, names, rgister_user))

    cursor.execute('''CREATE TABLE IF NOT EXISTS top_user_pincode(states varchar(200),
                                                                year int,
                                                                quater int,
                                                                name varchar(200),
                                                                registered_user bigint ) ''')

    for i in range(len(data_list_pincode1['State'])):
        state = data_list_pincode1['State'][i]
        year = data_list_pincode1['Year'][i]
        quarter = data_list_pincode1['Quater'][i]
        names = data_list_pincode1['Name'][i]
        rgister_user = data_list_pincode1['Registered_User'][i]
        
        cursor.execute('''INSERT INTO top_user_pincode(states, year, quater, name, registered_user)
                        VALUES(%s, %s, %s, %s, %s)''',
                    (state, year, quarter, names, rgister_user))

    mydb.commit()

    # Create DataFrames from the collected data
    top_user_state = pd.DataFrame(data_list_state1)
    top_user_districts = pd.DataFrame(data_list_districts1)
    top_user_pincode = pd.DataFrame(data_list_pincode1)

    st.write("Phonepe")


    mydb.close()
