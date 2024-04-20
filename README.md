# project_phonepe
Phonepe Pulse Data Exploration and Transaction


The Phonepe Pusle Data Visualization and Exploration is a Python-based project that extracts data from the Phonepe Pulse Github repository, transforms and stores it in a MySQL database , and displays it through an interactive dashboard using Streamlit, Plotly and few other visualization and data manipulation libraries. The solution includes with various visualizations, allowing users to select different facts and figures to display. The project is efficient, secure, and user-friendly, providing valuable insights and information about the data in the Phonepe Pulse Github repository from the year 2018 -2023.

Table of Contents:

PACKAGES
INSTALLATION
USAGE
FEATURES
LICENSE
CONTACT

PACKAGES:

import os
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import requests
import json
import plotly.colors
import plotly.graph_objects as go
from streamlit_option_menu import option_menu

INSTALLATION:

This project requires the following components:
STREAMLIT
MySQL
PANDAS
PLOTLY
  
  STREAMLIT:
  Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.
  
  PYDECK:
  Pydeck is a Python library tailored for interactive 3D mapping and geospatial visualization. Leveraging Deck.gl, it simplifies complex data visualization tasks with an intuitive interface. Pydeck is ideal for creating interactive maps with various layers and features, making it a powerful tool for geospatial analysis and exploration
  
  MySQL:
  MySQL is an open-source relational database management system (RDBMS) that is widely used for managing and organizing data. It is known for its reliability, scalability, and ease of use. MySQL uses a client-server model and is compatible with various programming languages, making it a popular choice for web applications.
  
  PANDAS:
  Matplotlib is a 2D plotting library for creating static, animated, and interactive visualizations in Python. It provides a wide variety of plotting options and customization features, making it a powerful tool for data visualization. Matplotlib is often used for creating charts, graphs, histograms, and other types of plots.
  
  PLOTLY:
  Plotly is a versatile Python library renowned for crafting interactive, publication-quality visualizations. Offering a wide array of charts, graphs, and dashboards, Plotly enables seamless customization and integration within Python environments. Its strength lies in interactivity, empowering users to create dynamic plots with ease, suitable for both exploratory data analysis and presentation purposes.

USAGE:

To run this project, follow these steps:

Clone the repository: https://github.com/Meenakshi050394/project_phonepe
Access the app in your browser at http://localhost:8501

FEATURES:

1. Visualization of transations all over country state and disrict along with the insights of data:

  The Pulse webpage will give you the overall visuvalization on the india map all the some basic insights like Total Transaction , Registered user and Apps open      based on year and quater all over the country or the select state.

2. Analyzing and Visualizing individual insights based on cloned data:

   Here we can explore individual analyzing and visualizing data from Aggrecated,Map,and Top Transaction and user, used plotly chart to visualizing the data easier 
   way.

3.View Top 10 Charts:

   Here we can show Top 10 Datas from all the tables.

Images uploaded in seprate file, 

LICENSE:


CONTACTS:

Email : meenakshi.sriram18@gmail.com
Linkedin : https://www.linkedin.com/in/meenakshihariharakrishnan/








