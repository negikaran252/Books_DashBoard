import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go

WAREHOUSE="Book_WAREHOUSE"
DATABASE="book_db"
SCHEMA="book_schema"
# Establish a connection to the Snowflake database
con = snowflake.connector.connect(
    user=st.secrets["USER_NAME"],
    password=st.secrets["PASSWORD"],
    account=st.secrets["ACCOUNT"],
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

# Create a cursor object
cur = con.cursor()

# SQL query to retrieve data
query = "SELECT * FROM Books"

# Execute the query and fetch the data
cur.execute(query)
data = cur.fetchall()

# Convert the data to a pandas DataFrame
df = pd.DataFrame(data, columns=[desc[0] for desc in cur.description])

# Close the cursor and the connection
cur.close()
con.close()

# Streamlit app
st.title('Books Dashboard - Powered by Streamlit')

total_books = len(df['TITLE'].unique())+1
average_price = df['PRICE'].mean()

# Create two columns
col1, col2 = st.columns(2)

# Display the total number of books in the first column
col1.markdown(f"""
    <div style="background-color: #C2D9FF; padding: 10px; border-radius: 10px; text-align:center;">
        <h3 style="color: #0F0F0F;">BOOKS</h3>
        <h1 style="color: #190482;"><b>{total_books}</b></h1>
    </div>
""", unsafe_allow_html=True)

# Display the average price in the second column
col2.markdown(f"""
    <div style="background-color: #C2D9FF; padding: 10px; border-radius: 10px; text-align:center;">
        <h3 style="color: #0F0F0F;">AVERAGE PRICE</h3>
        <h1 style="color: #190482;"><b>{average_price}</b></h1>
    </div>
""", unsafe_allow_html=True)

st.write("")
st.markdown("---")

# Create Pie Chart
# Calculate the number of books for each availability
availability_counts = df['AVAILABILITY'].value_counts()

# Create a DataFrame for the counts
df_counts = pd.DataFrame({'AVAILABILITY': ['In Stock', 'Out of Stock'], 'count': [availability_counts[True], 0]})

# Create a pie chart with plotly
fig = px.pie(df_counts, values='count', names='AVAILABILITY', color='AVAILABILITY',
             color_discrete_map={'In Stock':'green', 'Out of Stock':'red'})

# Center the title
fig.update_layout(
    title={
        'text': "In Stock vs Out of Stock",
        'y':1,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'})

# Display the chart in Streamlit
st.plotly_chart(fig)
st.markdown("---")

# Creating bar-Line Graph

# Calculate the total number of books for each rating
rating_counts = df['RATING'].value_counts().reset_index()
rating_counts.columns = ['RATING', 'COUNT']

# Calculate the mean price for each rating
mean_prices = df.groupby('RATING')['PRICE'].mean().reset_index()
mean_prices.columns = ['RATING', 'AVERAGE PRICE']

# Create a bar chart for the total number of books for each rating
fig = go.Figure()
fig.add_trace(go.Bar(x=rating_counts['RATING'], y=rating_counts['COUNT'], name='Number of Books'))

# Create a line chart for the mean price of each rating
fig.add_trace(go.Scatter(x=mean_prices['RATING'], y=mean_prices['AVERAGE PRICE'], name='Average Price', yaxis='y2'))

# Set the layout of the chart
fig.update_layout(
    autosize=False,
    width=700,
    height=500,
    xaxis=dict(title='Ratings'),
    yaxis=dict(title='Number of Books'),
    yaxis2=dict(title='Average Price', overlaying='y', side='right'),
    legend=dict(x=0.9, y=1.2)
)

# Display the chart in Streamlit
st.plotly_chart(fig)

st.markdown("---")
# Create tables for top 5 and lowest 5 books
# Sort the DataFrame by price in descending order
df_sorted = df.sort_values('PRICE', ascending=False)
col1, col2 = st.columns(2)

# Create a button for the top 5 books with the highest prices
if col1.button('Most Expensive Books'):
    # Display the top 5 books with the highest prices
    st.write(df_sorted[['TITLE', 'PRICE']].head(5).to_html(index=False), unsafe_allow_html=True)

# Create a button for the top 5 books with the lowest prices
if col2.button('Least Expensive Books'):
    # Display the top 5 books with the lowest prices
    st.write(df_sorted[['TITLE', 'PRICE']].tail(5).to_html(index=False), unsafe_allow_html=True)

st.markdown("""
    <style>
        .stButton>button {
            width: 100%;
            color: black;
            background-color: #C2D9FF;
        }
        .element-container {
            display: flex;
            justify-content: center;
        }
        table {
            margin-left: auto;
            margin-right: auto;
        }
    </style>
""", unsafe_allow_html=True)
st.markdown("---")
# Create a dropdown list of book titles
title = st.selectbox('Select a Book', df['TITLE'])

# Create a button
if st.button('View'):
    # Filter the DataFrame for the selected book
    book = df[df['TITLE'] == title]

    # Display the price, rating, and availability of the selected book
    st.write('Price: ', book['PRICE'].values[0])
    st.write('Rating: ', book['RATING'].values[0])
    st.write('Availability: ', 'In Stock' if book['AVAILABILITY'].values[0] else 'Out of Stock')




