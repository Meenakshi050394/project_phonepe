#import packages

import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import requests
import json
import plotly.colors
import plotly.graph_objects as go
from streamlit_option_menu import option_menu




#Homepage main

st.set_page_config(page_title= "Phonepe Pulse Data Visualization",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This dashboard app is created for PhonePe Data Visualization!
                                        Data has been cloned from Phonepe Pulse Github Repository"""})

st.sidebar.header(":violet[**Welcome to the dashboard!**]")
st.title(":violet[PHONEPE PULSE DATA VISUALIZATION]")




#Dataframe Creation

#SQL Connection
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="phonepe1"
)

cursor = mydb.cursor(buffered=True)

#Agg_Transacion_DF
cursor.execute("SELECT * FROM agg_trasaction")
mydb.commit()
tb1 = cursor.fetchall()
cursor.close()  
agg_trans = pd.DataFrame(tb1,columns=("State","Year","Quater","Type","Count","Amount"))

cursor = mydb.cursor(buffered=True)

#Agg_User_DF
cursor.execute("SELECT * FROM agg_user")
mydb.commit()
tb2 = cursor.fetchall()
cursor.close()  
agg_user = pd.DataFrame(tb2,columns=("State","Year","Quater","Register_user","Apps_Open","Brand","Count","Percentage"))

cursor = mydb.cursor(buffered=True)

#Map_Transaction_DF
cursor.execute("SELECT * FROM map_transaction")
mydb.commit()
tb3 = cursor.fetchall()
cursor.close()  
map_trans = pd.DataFrame(tb3,columns=("State","Year","Quater","Districts","Count","Amount"))

cursor = mydb.cursor(buffered=True)

#Map_User_DF
cursor.execute("SELECT * FROM map_user")
mydb.commit()
tb4 = cursor.fetchall()
cursor.close()  
map_user = pd.DataFrame(tb4,columns=("State","Year","Quater","Districts","Register_user","Apps_Open"))

cursor = mydb.cursor(buffered=True)

#Top_Transaction_Pincode_DF
cursor.execute("SELECT * FROM top_tran_pincode")
mydb.commit()
tb7 = cursor.fetchall()
cursor.close()  
top_trans_pincode= pd.DataFrame(tb7,columns=("State","Year","Quater","Pincode","Count","Amount"))

cursor = mydb.cursor(buffered=True)

#Top_User_State_DF
cursor.execute("SELECT * FROM top_user_pincode")
mydb.commit()
tb10 = cursor.fetchall()
cursor.close()  
top_user_pincode= pd.DataFrame(tb10,columns=("State","Year","Quater","Pincode","Registered_user"))




# Function Creation

# overall transaction data

#Dataframe Creation

#SQL Connection
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="phonepe1"
)

cursor = mydb.cursor(buffered=True)

#Agg_Transacion_DF
cursor.execute("SELECT state,year,quater,count,amount FROM agg_trasaction")
mydb.commit()
tb1 = cursor.fetchall()
cursor.close()  
agg_trans_in = pd.DataFrame(tb1,columns=("State","Year","Quater","Count","Amount"))

cursor = mydb.cursor(buffered=True)

#Map_Transacion_DF
cursor.execute("SELECT state,year,quater,count,amount FROM map_transaction")
mydb.commit()
tb2 = cursor.fetchall()
cursor.close()  
map_trans_in = pd.DataFrame(tb2,columns=("State","Year","Quater","Count","Amount"))

cursor = mydb.cursor(buffered=True)

#Top_Transacion_DF
cursor.execute("SELECT state,year,quater,count,amount FROM top_tran_pincode")
mydb.commit()
tb3 = cursor.fetchall()
cursor.close()  
top_trans_in = pd.DataFrame(tb3,columns=("State","Year","Quater","Count","Amount"))

overall_transaction= pd.concat([agg_trans_in,map_trans_in,top_trans_in], ignore_index=True)

cursor = mydb.cursor(buffered=True)

#Agg_Transacion_Type_DF

cursor.execute("SELECT name,count FROM agg_trasaction")
mydb.commit()
tb4 = cursor.fetchall()
cursor.close()
agg_trans_type_in = pd.DataFrame(tb4, columns=("Name", "Count"))


#Functions

def overall_data(df=agg_trans_type_in): 
    
    agg_trans_type_in_g = df.groupby("Name")[["Count"]].sum().reset_index()  

    st.markdown(f"""<h6 style="margin: 0;">OverAll PhonePe Transaction Type </h6>""", unsafe_allow_html=True)

    for idx, row in agg_trans_type_in_g.iterrows():
        transaction_type = row["Name"]
        count_value = row["Count"]

        st.markdown(f"""<h5 style='color:#b069ff; margin: 0;'>{transaction_type}: {count_value}</h5>""", unsafe_allow_html=True)
    

def overall_trans(type, years, quaters):

    tran_ac_y=type[(type["Year"] == years) & (type["Quater"]== quaters)]
    tran_ac_y.reset_index(drop=True,inplace=True)

    tran_ac_y_grp=tran_ac_y.groupby("State")[["Count","Amount"]].sum()
    tran_ac_y_grp.reset_index(inplace=True)

    url= r"https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data=json.loads(response.content)
    states_names=[]
    color_scale = px.colors.sequential.Plasma
    for feature in data["features"]:
        states_names.append(feature["properties"]["ST_NM"])
    
    states_names.sort()

    st.markdown("## :violet[Overall State Data - Transactions Amount and Count]")

    fig_india=px.choropleth(tran_ac_y_grp, geojson= data, locations="State", featureidkey="properties.ST_NM",
                            color="Amount", color_continuous_scale=color_scale, range_color=(tran_ac_y_grp["Amount"].min(),tran_ac_y_grp["Amount"].max()),
                            hover_data=["Count","Amount"], fitbounds="locations", height=600, width=600)
    
    
    fig_india.update_geos(visible=False)
    fig_india.update_layout(plot_bgcolor='rgba(0, 0, 0, 0)')
    st.plotly_chart(fig_india)


# overall User data

#Dataframe Creation

#SQL Connection
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="phonepe1"
)

cursor = mydb.cursor(buffered=True)

#Agg_RgUser_DF
cursor.execute("SELECT state,year,quater,registered_user FROM agg_user")
mydb.commit()
tb1 = cursor.fetchall()
cursor.close()  
agg_Rguser_in = pd.DataFrame(tb1,columns=("State","Year","Quater","Registered_User"))

cursor = mydb.cursor(buffered=True)

#Map_RgUs_DF
cursor.execute("SELECT state,year,quater,registered_user FROM map_user")
mydb.commit()
tb2 = cursor.fetchall()
cursor.close()  
map_Rguser_in = pd.DataFrame(tb2,columns=("State","Year","Quater","Registered_User"))

cursor = mydb.cursor(buffered=True)

#Top_RgUser_DF
cursor.execute("SELECT state,year,quater,registered_user FROM top_user_pincode")
mydb.commit()
tb3 = cursor.fetchall()
cursor.close()  
top_Rguser_in = pd.DataFrame(tb3,columns=("State","Year","Quater","Registered_User"))

overall_Rguser= pd.concat([agg_Rguser_in,map_Rguser_in,top_Rguser_in], ignore_index=True)



cursor = mydb.cursor(buffered=True)

#Agg_AppsUser_DF
cursor.execute("SELECT state,year,quater,apps_open FROM agg_user")
mydb.commit()
tb1 = cursor.fetchall()
cursor.close()  
agg_userapps_in = pd.DataFrame(tb1,columns=("State","Year","Quater","Apps_Open"))

cursor = mydb.cursor(buffered=True)

#Map_AppsUs_DF
cursor.execute("SELECT state,year,quater,apps_open FROM map_user")
mydb.commit()
tb2 = cursor.fetchall()
cursor.close()  
map_userapps_in = pd.DataFrame(tb2,columns=("State","Year","Quater","Apps_Open"))

cursor = mydb.cursor(buffered=True)

overall_Appsuser= pd.concat([agg_userapps_in,map_userapps_in], ignore_index=True)

# Functions 

def overall_Rgusers(type,years,quaters):

    tran_ac_y=type[(type["Year"] == years) & (type["Quater"]== quaters)]
    tran_ac_y.reset_index(drop=True,inplace=True)

    tran_ac_y_grp=type.groupby("State")[["Registered_User"]].sum()
    tran_ac_y_grp.reset_index(inplace=True)

    url= r"https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data=json.loads(response.content)
    states_names=[]
    color_scale = px.colors.sequential.Plasma
    for feature in data["features"]:
        states_names.append(feature["properties"]["ST_NM"])
    
    states_names.sort()

    st.markdown("## :violet[Overall State Data - Registered User ]")

    fig_india_user=px.choropleth(tran_ac_y_grp, geojson= data, locations="State", featureidkey="properties.ST_NM",
                            color="Registered_User", color_continuous_scale=color_scale, range_color=(tran_ac_y_grp["Registered_User"].min(),tran_ac_y_grp["Registered_User"].max()),
                            hover_data=["Registered_User"],  fitbounds="locations", height=600, width=600)
    
    fig_india_user.update_geos(visible=False)
    
    st.plotly_chart(fig_india_user)

def overall_Appsopen(type,years,quaters):

    tran_ac_y=type[(type["Year"] == years) & (type["Quater"]== quaters)]
    tran_ac_y.reset_index(drop=True,inplace=True)

    tran_ac_y_grp=type.groupby("State")[["Apps_Open"]].sum()
    tran_ac_y_grp.reset_index(inplace=True)

    url= r"https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data=json.loads(response.content)
    states_names=[]
    color_scale = px.colors.sequential.Plasma
    for feature in data["features"]:
        states_names.append(feature["properties"]["ST_NM"])
    
    states_names.sort()

    st.markdown("## :violet[Overall State Data - User Apps Open]")

    fig_india_user=px.choropleth(tran_ac_y_grp, geojson= data, locations="State", featureidkey="properties.ST_NM",
                            color="Apps_Open", color_continuous_scale=color_scale, range_color=(tran_ac_y_grp["Apps_Open"].min(),tran_ac_y_grp["Apps_Open"].max()),
                            hover_data=["Apps_Open"], fitbounds="locations", height=600, width=600)
    
    fig_india_user.update_geos(visible=False)
    
    st.plotly_chart(fig_india_user)


    

#Data Visualisazion part


def agg_transaction_type(df,state,year,quater):

    col1, col2 = st. columns(2)

    with col1:

        mask = (df["State"]==state) & (df["Year"] == year) & (df["Quater"] == quater)  
        tran_ac_y = agg_trans[mask]

        tran_ac_y.reset_index(drop=True, inplace=True)

        tran_ac_y_grp = tran_ac_y.groupby("Type")[["Count", "Amount"]].sum()
        tran_ac_y_grp.reset_index(inplace=True)

        title_html = f"<span style='color:violet'><b>TRANSACTION COUNT</b></span>"\
                f"<br>" \
                f"<span style='color:violet'><b>STATE:</b> {state} </span> " \
                f"<span style='color:violet'><b>YEAR:</b> {year} </span> " \
                f"<span style='color:violet'><b>QUARTER:</b> {quater} </span>"

        fig_pie_1=px.pie(data_frame=tran_ac_y_grp, names="Type", values="Count",
                        width=600, title= title_html, hole = 0.5)
        st.plotly_chart(fig_pie_1)

    with col2:

        title_html = f"<span style='color:violet'><b>TRANSACTION AMOUNT</b></span>"\
                f"<br>" \
                f"<span style='color:violet'><b>STATE:</b> {state} </span> " \
                f"<span style='color:violet'><b>YEAR:</b> {year} </span> " \
                f"<span style='color:violet'><b>QUARTER:</b> {quater} </span>"

        fig_pie_2=px.pie(data_frame=tran_ac_y_grp, names="Type", values="Amount",
                        width=600, title= title_html, hole = 0.5)
        st.plotly_chart(fig_pie_2)



def agg_user_type(df,state,year,quater):
        
    col1,col2 = st.columns(2)

    with col1:

        mask = (df["State"]==state) & (df["Year"] == year) & (df["Quater"] == quater)  
        user_cp_b = agg_user[mask]

        user_cp_b.reset_index(drop=True, inplace=True)

        user_cp_b_grp=user_cp_b.groupby('Brand')[["Count","Percentage"]].sum()
        user_cp_b_grp.reset_index(inplace=True)

        title_html = f"<span style='color:violet'><b>BRAND WISE USER COUNT</b></span>"\
                f"<br>" \
                f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_pie_1=px.pie(data_frame=user_cp_b_grp, names="Brand", values="Count",
                        width=600, title= title_html, hole = 0.5)
        st.plotly_chart(fig_pie_1)
    
    with col2:

        title_html = f"<span style='color:violet'><b>BRAND WISE USER PERCENTAGE</b></span>"\
                f"<br>" \
                f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_pie_2=px.pie(data_frame=user_cp_b_grp, names="Brand", values="Percentage",
                        width=600, title= title_html, hole = 0.5)
        st.plotly_chart(fig_pie_2)



def agg_user_type2(df,year,quater):

    col1, col2 = st.columns(2)

    with col1:

        mask = (df["Year"] == year) & (df["Quater"] == quater)  
        user_cp_b = agg_user[mask]

        user_cp_b_grp1=user_cp_b.groupby('State')[["Register_user","Apps_Open"]].sum()
        user_cp_b_grp1.reset_index(inplace=True)

        amount_color_scale = px.colors.sequential.amp_r
        count_color_scale = px.colors.sequential.Oranges_r

        title_html = f"<span style='color:violet'><b>REGISTER USER</b></span>"\
                f"<br>" \
                f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_reg = px.bar(
        user_cp_b_grp1, x="State", y="Register_user",
        title=title_html,
        color_discrete_sequence=amount_color_scale, height=650, width=600
        )
        st.plotly_chart(fig_reg)

    with col2:

        title_html = f"<span style='color:violet'><b>APPS OPEN</b></span>"\
                f"<br>" \
                f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_app = px.bar(
        user_cp_b_grp1, x="State", y="Apps_Open",
        title=title_html,
        color_discrete_sequence=count_color_scale, height=650, width=600
        )
        st.plotly_chart(fig_app)



def Map_Transaction_type(df,state, year, quater):
    
    col1,col2 = st.columns(2)

    with col1:

        mask = (df["State"]==state) & (df["Year"] == year) & (df["Quater"] == quater)
        tran_ac_y = df[mask]

        tran_ac_y.reset_index(drop=True, inplace=True)

        tran_ac_y_grp = tran_ac_y.groupby("Districts")[["Count", "Amount"]].sum()
        tran_ac_y_grp.reset_index(inplace=True)


        title_html = f"<span style='color:violet'><b>TRANSACTION COUNT</b></span>"\
                        f"<br>" \
                        f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                        f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                        f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_count = px.bar(
            tran_ac_y_grp, x="Districts", y="Count",
            title=title_html,
            color_discrete_sequence=px.colors.sequential.Magenta_r, height=650, width=600
        )
        st.plotly_chart(fig_count)

    with col2:
        
        title_html = f"<span style='color:violet'><b>TRANSACTION AMOUNT</b></span>"\
                        f"<br>" \
                        f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                        f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                        f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_amount = px.bar(
            tran_ac_y_grp, x="Districts", y="Amount",
            title=title_html,
            color_discrete_sequence=px.colors.sequential.PuBuGn_r, height=650, width=600
        )
        st.plotly_chart(fig_amount)




def Map_User_type(df,state, year, quater):

    col12,col13 = st.columns(2)

    with col12:

        mask = (df["State"]==state) & (df["Year"] == year) & (df["Quater"] == quater)
        tran_ac_y = df[mask]

        tran_ac_y.reset_index(drop=True, inplace=True)

        tran_ac_y_grp = tran_ac_y.groupby("Districts")[["Register_user", "Apps_Open"]].sum()
        tran_ac_y_grp.reset_index(inplace=True)


        title_html = f"<span style='color:violet'><b>REGISTER USER</b></span>"\
                        f"<br>" \
                        f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                        f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                        f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_scatter1 = px.scatter(
        tran_ac_y_grp, x="Districts", y="Register_user", color="Districts",
        title=title_html, height=650, width=600
        )
        st.plotly_chart(fig_scatter1)

    with col13:

        title_html = f"<span style='color:violet'><b>APPS OPEN</b></span>"\
                        f"<br>" \
                        f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                        f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                        f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_scatter2 = px.scatter(
        tran_ac_y_grp, x="Districts", y="Apps_Open", color="Districts",
        title=title_html, height=650, width=600
        )
        st.plotly_chart(fig_scatter2)



def Top_Transaction_type(df,state, year, quater):
  
    col17, col18 = st.columns(2)

    with col17:

        mask = (df["State"]==state) & (df["Year"] == year) & (df["Quater"] == quater)
        tran_ac_y = df[mask]

        tran_ac_y.reset_index(drop=True, inplace=True)

        #tran_ac_y_grp = tran_ac_y.groupby("Pincode")[["Count", "Amount"]].sum()
        #tran_ac_y_grp.reset_index(inplace=True)

        title_html = f"<span style='color:violet'><b>TRANSACTION COUNT</b></span>"\
                        f"<br>" \
                        f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                        f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                        f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"
        
        plasma_colorscale = plotly.colors.sequential.Plasma

        fig_count = px.bar(
            tran_ac_y, x="Quater", y="Count", hover_data="Pincode",
            title=title_html,
            color=plasma_colorscale,
            height=650, width=600
        )
        fig_count.update_yaxes(tickformat=".0f")
        st.plotly_chart(fig_count)
    with col18:

        title_html = f"<span style='color:violet'><b>TRANSACTION AMOUNT</b></span>"\
                        f"<br>" \
                        f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                        f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                        f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"

        fig_amount = px.bar(
            tran_ac_y, x="Quater", y="Amount",hover_data="Pincode",
            title=title_html,
            color=plasma_colorscale,
            height=650, width=600
        )
        fig_count.update_yaxes(tickformat=".0f")
        st.plotly_chart(fig_amount)




def Top_User_type(df,state, year, quater):

    col22, col23 = st.columns(2)

    with col22:

        mask = (df["State"]==state) & (df["Year"] == year) & (df["Quater"] == quater)
        tran_ac_y = df[mask]

        tran_ac_y.reset_index(drop=True, inplace=True)

        #tran_ac_y_grp = tran_ac_y.groupby("Pincode")[["Count", "Amount"]].sum()
        #tran_ac_y_grp.reset_index(inplace=True)

        title_html = f"<span style='color:violet'><b>REGISTERED USER</b></span>"\
                        f"<br>" \
                        f"<span style='color:violet'><b>STATE:</b>{state}</span> " \
                        f"<span style='color:violet'><b>YEAR:</b>{year}</span> " \
                        f"<span style='color:violet'><b>QUARTER:</b>{quater}</span>"
        
        plasma_colorscale = plotly.colors.sequential.Plasma

        fig_register_data = px.line(
                tran_ac_y, x="Pincode", y="Registered_user", hover_data="Pincode",
                title=title_html,
                color=plasma_colorscale,
                height=650, width=600,
                markers={'size': 8},
                line_shape='linear')

        fig_register_data.update_xaxes(tickformat=".0f")
        st.plotly_chart(fig_register_data)





#SQL Connection - Top chart transaction

def top_chart_trans_amount_count(table_name):

    
    mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    password="root",
    database="phonepe1"
)

    cursor = mydb.cursor(buffered=True)

    col27,col28 = st.columns(2)

    with col27:

        #plot1
        Query_1=f'''SELECT state, SUM(amount) AS Transaction_Amount 
                FROM {table_name} 
                GROUP BY state
                ORDER BY Transaction_Amount DESC 
                LIMIT 10;'''
        cursor.execute(Query_1)
        fetch=cursor.fetchall()
        mydb.commit()

        df_AT=pd.DataFrame(fetch,columns=("State","Transaction_Amount"))


        fig_amout_Top_chart=px.bar(df_AT, x="State", y="Transaction_Amount", title="TOP 10 TRANSACTION AMOUNT", 
                            color_discrete_sequence=px.colors.sequential.RdBu_r, height=650, width=600)
        st.plotly_chart(fig_amout_Top_chart)

    with col28:

        #plot2
        Query_2=f'''SELECT state, AVG(amount) AS Transaction_Amount 
                FROM {table_name}
                GROUP BY state
                ORDER BY Transaction_Amount;'''
                
        cursor.execute(Query_2)
        fetch_2=cursor.fetchall()
        mydb.commit()

        df_AT_2=pd.DataFrame(fetch_2,columns=("State","Transaction_Amount"))


        fig_amout_Top_Chart2=px.bar(df_AT_2, y="State", x="Transaction_Amount", title="AVERAGE TRANSACTION AMOUNT", orientation="h",
                            color_discrete_sequence=px.colors.sequential.Purpor_r, height=650, width=600)
        st.plotly_chart(fig_amout_Top_Chart2)


    col29,col30 = st.columns(2)

    with col29:

        #plot3
        Query_3=f'''SELECT state, SUM(count) AS Transaction_Count
                FROM {table_name}
                GROUP BY state
                ORDER BY Transaction_Count DESC
                LIMIT 10;'''
                
        cursor.execute(Query_3)
        fetch_3=cursor.fetchall()
        mydb.commit()

        df_AT_3=pd.DataFrame(fetch_3,columns=("State","Transaction_Count"))


        fig_count_3=px.pie(data_frame=df_AT_3, names='State',values="Transaction_Count", title="TOP 10 TRANSACTION COUNT",
                        height=650, width=600,hole=0.5)
        st.plotly_chart(fig_count_3)

    with col30:

        #plot4
        Query_4=f'''SELECT state, AVG(count) AS Transaction_Count
                FROM {table_name}
                GROUP BY state
                ORDER BY Transaction_Count;'''
                
        cursor.execute(Query_4)
        fetch_4=cursor.fetchall()
        mydb.commit()

        df_AT_4=pd.DataFrame(fetch_4,columns=("State","Transaction_Count"))


        fig_count_4=px.pie(data_frame=df_AT_4, names='State',values="Transaction_Count", title="AVERAGE TRANSACTION COUNT",
                        height=650, width=600,hole=0.5)
        st.plotly_chart(fig_count_4)



#SQL Connection -- Top Chart User


def top_chart_user_brandwise(table_name):

        mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe1"
    )
    
        cursor = mydb.cursor()

        col31,col32 = st.columns(2)

        with col31:

            #plot1
            Query_1=f'''SELECT user_brand, SUM(user_count) AS User_Count 
                    FROM {table_name} 
                    GROUP BY user_brand
                    ORDER BY User_Count DESC 
                    LIMIT 10;'''
            cursor.execute(Query_1)
            fetch_1=cursor.fetchall()
            mydb.commit()

            df_AU=pd.DataFrame(fetch_1,columns=("User_Brand","User_Count"))

            fig_amout_AU=px.bar(df_AU, x="User_Count", y="User_Brand", title="TOP 10 BRANDWISE COUNT",orientation="h",
                                    color_discrete_sequence=px.colors.sequential.BuPu_r , height=650, width=600)
            st.plotly_chart(fig_amout_AU)

        with col32:

            #plot2
            
            Query_2=f'''SELECT user_brand, AVG(user_count) AS User_Count 
                            FROM {table_name} 
                            GROUP BY user_brand
                            ORDER BY User_Count'''
            cursor.execute(Query_2)
            fetch_2=cursor.fetchall()
            mydb.commit()

            df_AU_1=pd.DataFrame(fetch_2,columns=("User_Brand","User_Count"))

            fig_amout_AU1=px.bar(df_AU_1, y="User_Count", x="User_Brand", title="AVERAGE OF BRANDWISE COUNT",
                                    color_discrete_sequence=px.colors.sequential.Peach_r , height=650, width=600)
            st.plotly_chart(fig_amout_AU1)

        col33,col34 = st.columns(2)

        with col33:

            #plot3
            Query_3=f'''SELECT user_brand, SUM(user_percentage) AS User_Percentage
                    FROM {table_name}
                    GROUP BY user_brand
                    ORDER BY User_Percentage DESC
                    LIMIT 10;'''
                    
            cursor.execute(Query_3)
            fetch_3=cursor.fetchall()
            mydb.commit()

            df_AU_3=pd.DataFrame(fetch_3,columns=("User_Brand","User_Percentage"))


            fig_percent_1=px.pie(data_frame=df_AU_3, names='User_Brand',values="User_Percentage", title="TOP 10 BRANDWISE PERCENTAGE",
                            height=650, width=600,hole=0.5)
            st.plotly_chart(fig_percent_1)

        with col34:

            #plot4
            Query_4=f'''SELECT user_brand, AVG(user_percentage) AS User_Percentage
                    FROM {table_name}
                    GROUP BY user_brand
                    ORDER BY User_Percentage'''
                    
            cursor.execute(Query_4)
            fetch_4=cursor.fetchall()
            mydb.commit()

            df_AU_4=pd.DataFrame(fetch_4,columns=("User_Brand","User_Percentage"))


            fig_percent_2=px.pie(data_frame=df_AU_4, names='User_Brand',values="User_Percentage", title="AVERAGE OF BRANDWISE PERCENTAGE",
                            height=650, width=600,hole=0.5)
            st.plotly_chart(fig_percent_2)




#SQL Connection

#Top10_user

def top_chart_user_Reguser_appsopen(table_name):

        mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe1"
    )
    
        cursor = mydb.cursor()

        col31,col32 = st.columns(2)

        with col31:

            #plot1
            Query_1=f'''SELECT state, year, SUM(registered_user) AS Registered_User 
                    FROM {table_name}
                    GROUP BY state,year
                    ORDER BY Registered_User DESC 
                    LIMIT 10;'''
            cursor.execute(Query_1)
            fetch_1=cursor.fetchall()
            mydb.commit()

            df_AU=pd.DataFrame(fetch_1,columns=("State","Year","Registered_User"))

            fig_amout_AU=px.line_3d(data_frame=df_AU,x="State",y="Year",z="Registered_User",title="TOP 10 REGISTERED USER",
                                    markers=dict(color='violet', width=3),
                                    height=650,width=600)
            st.plotly_chart(fig_amout_AU)

        with col32:

            #plot2
            Query_2=f'''SELECT state, year, AVG(registered_user) AS Registered_User 
                    FROM {table_name}
                    GROUP BY state,year
                    ORDER BY Registered_User'''
            cursor.execute(Query_2)
            fetch_2=cursor.fetchall()
            mydb.commit()

            df_AU_1=pd.DataFrame(fetch_2,columns=("State","Year","Registered_User"))

            fig_amout_AU1=px.histogram(data_frame=df_AU_1,x="State",y="Registered_User",color="Registered_User",
                                       color_discrete_sequence=px.colors.sequential.Viridis_r,
                                       title="AVERAGE REGISTERED USER",height=650,width=600)
            st.plotly_chart(fig_amout_AU1)

        col33, col34 = st.columns(2)

        with col33:

            #plot3
            Query_3=f'''SELECT state,year, SUM(apps_open) AS Apps_open
                    FROM {table_name}
                    GROUP BY state,year
                    ORDER BY Apps_open DESC
                    LIMIT 10;'''
                    
            cursor.execute(Query_3)
            fetch_3=cursor.fetchall()
            mydb.commit()

            df_AU_3=pd.DataFrame(fetch_3,columns=("State","Year","Apps_open"))


            fig_apps_1=px.scatter_3d(data_frame=df_AU_3, x='State',y="Apps_open",z="Year", title="TOP 10 APPS OPEN",hover_data='Apps_open',
                            height=650, width=600, color_discrete_sequence=px.colors.sequential.Cividis_r)
            st.plotly_chart(fig_apps_1)

        with col34:

            #plot4
            Query_4=f'''SELECT state, AVG(apps_open) AS Apps_open
                    FROM {table_name}
                    GROUP BY state
                    ORDER BY Apps_open'''
                    
            cursor.execute(Query_4)
            fetch_4=cursor.fetchall()
            mydb.commit()

            df_AU_4=pd.DataFrame(fetch_4,columns=("State","Apps_open"))


            fig_apps_2=px.histogram(data_frame=df_AU_4, x='State',y="Apps_open", title="AVERAGE APPS OPEN",
                            height=650, width=600, color_discrete_sequence=px.colors.sequential.YlOrRd_r)
            st.plotly_chart(fig_apps_2)




#SQL Connection

#Top10_user_1

def top_chart_user_Reguser(table_name):

        mydb = mysql.connector.connect(
        host="127.0.0.1",
        port="3306",
        user="root",
        password="root",
        database="phonepe1"
    )
    
        cursor = mydb.cursor()

        col31,col32 = st.columns(2)

        with col31:

            #plot1
            Query_1=f'''SELECT state, year, SUM(registered_user) AS Registered_User 
                    FROM {table_name}
                    GROUP BY state,year
                    ORDER BY Registered_User DESC 
                    LIMIT 10;'''
            cursor.execute(Query_1)
            fetch_1=cursor.fetchall()
            mydb.commit()

            df_AU=pd.DataFrame(fetch_1,columns=("State","Year","Registered_User"))

            fig_amout_AU=px.line_3d(data_frame=df_AU,x="State",y="Year",z="Registered_User",title="TOP 10 REGISTERED USER",
                                    markers=dict(color='violet', width=3),height=650,width=600)
            fig_amout_AU.update_layout(yaxis={'tickformat': '.2f'})
            st.plotly_chart(fig_amout_AU)

        with col32:

            #plot2
            Query_2=f'''SELECT state, year, AVG(registered_user) AS Registered_User 
                    FROM {table_name}
                    GROUP BY state,year
                    ORDER BY Registered_User'''
            cursor.execute(Query_2)
            fetch_2=cursor.fetchall()
            mydb.commit()

            df_AU_1=pd.DataFrame(fetch_2,columns=("State","Year","Registered_User"))

            fig_amout_AU1=px.histogram(data_frame=df_AU_1,x="State",y="Registered_User",color="Registered_User",
                                       color_discrete_sequence=px.colors.sequential.Rainbow_r,
                                       title="AVERAGE REGISTERED USER",height=650,width=600)
            st.plotly_chart(fig_amout_AU1)




# Streamlit Part


with st.sidebar:
    select=option_menu("Main_Menu",["Home","Data Analysis","Top Visualization"])

# Home Page
if select=="Home":

    main,sub = st.columns(2)
    
    
    with main:

        col1,col2,col3 = st.columns(3)

        with col1:
            type=st.selectbox("Select the Type",["Transaction",
                                                 "Registered User",
                                                 "Apps Open"])
        with col2:
            years = st.selectbox("Year",overall_transaction['Year'].unique())

        with col3:
            Quaters = st.selectbox("Quater",overall_transaction['Quater'].unique())

    if type == "Transaction":

        one,two = st.columns(2)

        with one:

            ovall_tran_count = overall_transaction['Count'].sum() 
            ovall_tran_amt = overall_transaction['Amount'].sum()  

            if ovall_tran_count > 0:
                avg_trans = ovall_tran_amt / ovall_tran_count
            else:
                avg_trans = 0  

        
            #total_count_formatted = '{:,.2f}'.format(ovall_tran_count)
            total_amount_formatted = '{:,.2f}'.format(ovall_tran_amt)
            avg_amount_formatted = '{:,.2f}'.format(avg_trans)

            total_payment = '₹' + total_amount_formatted
            avg_payment = '₹' + avg_amount_formatted

            st.markdown(f"""<p style='margin: 0;'>All PhonePe transactions (UPI + Cards + Wallets)</p>
                        <h4 style='color:#b069ff; margin: 0;'>{ovall_tran_count}</h4>
                        <p style='margin: 0;'>Total payment value</p>
                        <h4 style='color:#b069ff; margin: 0;'>{total_payment}</h4>
                        <p style='margin: 0;'>Avg. transaction value</p>
                        <h4 style='color:#b069ff; margin: 0;'>{avg_payment}</h4>
                        """, unsafe_allow_html=True)

        with two:

            overall_data(agg_trans_type_in)

        overall_trans(overall_transaction,years,Quaters)


    elif type == "Registered User":

        ovall_Rguser_count = overall_Rguser['Registered_User'].sum()  
       
        #total_regis_user_formetted = '{:,.2f}'.format(ovall_Rguser_count)


        st.markdown(f"""<p style='margin: 0;'>All PhonePe Registered User </p>
                    <h4 style='color:#b069ff; margin: 0;'>{ovall_Rguser_count}</h4>
                    """, unsafe_allow_html=True)

        overall_Rgusers(overall_Rguser,years,Quaters)

    elif type == "Apps Open":

        ovall_Appsopen_count = overall_Appsuser['Apps_Open'].sum()  
       
        #total__appsopen_formetted = '{:,.2f}'.format(ovall_Appsopen_count)

        st.markdown(f"""<p style='margin: 0;'>All PhonePe Apps Open</p>
                    <h4 style='color:#b069ff; margin: 0;'>{ovall_Appsopen_count}</h4>
                    """, unsafe_allow_html=True)

        overall_Appsopen(overall_Appsuser,years,Quaters)



   
# Data Ananlysis 

elif select=="Data Analysis":

    t1,t2,t3=st.tabs(["Aggrecated Analysis", "Map Analysis","Top Analysis"])

    with t1:

        method = st.radio("Select the Method",["Aggrecation Transaction Analysis","Aggrecation User Analysis"])

        if method == "Aggrecation Transaction Analysis":

            col01,col02,col03=st.columns(3)

            with col01:
                states1=st.selectbox("Select the State",agg_trans['State'].unique())
            with col02:
                years1=st.selectbox("Select the Year", agg_trans['Year'].unique())
            with col03:
                quaters1=st.selectbox("Select the Quater", agg_trans['Quater'].unique())

            agg_transaction_type(agg_trans,states1,years1,quaters1)

                
        elif method == "Aggrecation User Analysis":

            col04,col05,col06 = st.columns(3)

            with col04:
                states2=st.select_slider("Select the State",agg_user['State'].unique())
            with col05:
                years2=st.select_slider("Select the Year",agg_user['Year'].unique())
            with col06:
                quaters2=st.select_slider("Select the Quater",agg_user['Quater'].unique())

            agg_user_type(agg_user,states2,years2,quaters2)

            col07,col08 = st.columns(2)

            with col07:
                years3=st.selectbox("Select the Year",agg_user['Year'].unique())
            with col08:
                quaters3=st.selectbox("Select the Quater",agg_user['Quater'].unique())

            agg_user_type2(agg_user,years3,quaters3)



    with t2:

        method1 = st.radio("Select the Method",["Map Transaction Analysis","Map User Analysis"])

        if method1 == "Map Transaction Analysis":

            col09,col10,col11 = st.columns(3)

            with col09:
                states4=st.select_slider("Select the States MT",map_trans['State'].unique())
            with col10:
                years4=st.select_slider("Select the Years MT",map_trans['Year'].unique())
            with col11:
                quaters4=st.select_slider("Select the Quaters MT",map_trans['Quater'].unique())

            Map_Transaction_type(map_trans,states4, years4, quaters4)
            
        elif method1 == "Map User Analysis":
            
            col14,col15,col16 = st.columns(3)

            with col14:
                states5=st.selectbox("Select the Statess MU",map_user['State'].unique())
            with col15:
                years5=st.selectbox("Select the Yearss MU",map_user['Year'].unique())
            with col16:
                quaters5=st.selectbox("Select the Quaterss MU",map_user['Quater'].unique())

            Map_User_type(map_user,states5, years5, quaters5)


    with t3:

        method2 = st.radio("Select the Method",["Top Transaction Analysis","Top User Analysis"])

        if method2 == "Top Transaction Analysis":
            
            col19,col20,col21 = st.columns(3)

            with col19:
                states6=st.selectbox("Select the Statess TT",top_trans_pincode['State'].unique())
            with col20:
                years6=st.selectbox("Select the Yearss TT",top_trans_pincode['Year'].unique())
            with col21:
                quaters6=st.selectbox("Select the Quaterss TT",top_trans_pincode['Quater'].unique())

            Top_Transaction_type(top_trans_pincode,states6, years6, quaters6)


        elif method2 == "Top User Analysis":

            col24,col25,col26 = st.columns(3)

            with col24:
                states7=st.select_slider("Select the Statess TU",top_user_pincode['State'].unique())
            with col25:
                years7=st.select_slider("Select the Yearss TU",top_user_pincode['Year'].unique())
            with col26:
                quaters7=st.select_slider("Select the Quaterss TU",top_user_pincode['Quater'].unique())

            Top_User_type(top_user_pincode,states7, years7, quaters7)




# Top Visualization


elif select == "Top Visualization":
            st.title(":violet[View Top 10 Charts]")
            question=st.selectbox("Select The Questions",["1. Transaction Amount and Count from Aggrecated Transaction",
                                                "2. Transaction Amount and Count from Map Transaction",
                                                "3. Transaction Amount and Count from Top Transaction",
                                                "4. Brandwise Amount and Percentage from Aggrecated User",
                                                "5. Registered User and Apps open from Aggrecated User",
                                                "6. Registered User and Apps open  from Map User",
                                                "7. Registered User from Top User"])
            

            if question == "1. Transaction Amount and Count from Aggrecated Transaction":
                
                top_chart_trans_amount_count("agg_trasaction")

            elif question == "2. Transaction Amount and Count from Map Transaction":
                
                top_chart_trans_amount_count("map_transaction")

            elif question == "3. Transaction Amount and Count from Top Transaction":
                
                top_chart_trans_amount_count("top_tran_pincode")

            elif question == "4. Brandwise Amount and Percentage from Aggrecated User":

                top_chart_user_brandwise("agg_user")

            elif question == "5. Registered User and Apps open from Aggrecated User":

                top_chart_user_Reguser_appsopen("agg_user")

            elif question == "6. Registered User and Apps open  from Map User":

                top_chart_user_Reguser_appsopen("map_user")

            elif question == "7. Registered User from Top User":

                top_chart_user_Reguser("top_user_pincode")


mydb.close()






 


    







