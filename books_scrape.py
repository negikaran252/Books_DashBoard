import requests,os
from bs4 import BeautifulSoup
import csv
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

word_to_number = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5
}

books_data=[]
for page_num in range(1,51):
    url = f"https://books.toscrape.com/catalogue/page-{page_num}.html"
    response = requests.get(url)
    soup = BeautifulSoup(response.text,'html.parser')
    books = soup.find_all("article", class_="product_pod")
    # Iterate through the books and extract the information for each book
    for book in books:
        title = book.h3.a["title"]
        title=title.replace("\n", " ").replace(",", " ")    
        price = book.find("p", class_="price_color").text.strip()
        price=price[2:]
        rating = word_to_number[book.p["class"][1].lower()]
        stock = book.find("p", class_="instock availability").text.strip()
        flag=False
        if stock == "In stock":
            flag=True
        books_data.append([title,rating,price,flag])

with open('Books.csv', 'w', newline='',encoding='utf-8') as file:
    writer = csv.writer(file)
    headers = ["Title","Rating","Price","Availability"]
    writer.writerow(headers)
    # Write each sublist in the data to the CSV
    for sublist in books_data:
        writer.writerow(sublist)

# Connecting to SnowFlake

# Establish a connection to the Snowflake database
con = snowflake.connector.connect(
    user=os.getenv('USER_NAME'),
    password=os.getenv('PASSWORD'),
    account=os.getenv('ACCOUNT')
)

# Create a cursor object
cur = con.cursor()

# Create a warehouse
cur.execute("CREATE WAREHOUSE IF NOT EXISTS Book_WAREHOUSE")
cur.execute("USE WAREHOUSE Book_WAREHOUSE")

# Create a database
cur.execute("CREATE DATABASE IF NOT EXISTS book_db")
cur.execute("USE DATABASE book_db")

# Create a schema
cur.execute("CREATE SCHEMA IF NOT EXISTS book_schema")
cur.execute("USE SCHEMA book_schema")

# Create a table
table_query = """
CREATE OR REPLACE TABLE Books (
    Title VARCHAR,
    Rating INTEGER,
    Price FLOAT,
    Availability BOOLEAN
)
"""
cur.execute(table_query)

# creating a stage
cur.execute("CREATE OR REPLACE STAGE books_staging")

# loading the data from the csv file to the stage
cur.execute("PUT file://Books.csv @books_staging auto_compress=true")

# copying the data from the stage to the table
cur.execute("COPY INTO Books FROM @books_staging/  FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1)")

# Close the cursor and the connection
cur.close()
con.close()