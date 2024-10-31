import json
import pandas as pd
from sqlalchemy import create_engine

# Параметры подключения
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'secret',
    'host': 'localhost',
    'port': '5432'
}

connection_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
engine = create_engine(connection_string)

# 1-ый запрос
query1 = """
WITH sellersales AS (
    SELECT s.productkey, SUM(s.extendedamount::money) totalsellersales
    FROM factresellersales s
    WHERE s.orderdate BETWEEN '2010-10-01' AND '2010-12-31'
    group by s.productkey
),
internetsales AS (
    SELECT s.productkey, SUM(s.extendedamount::money) totalinternetsales
    FROM factinternetsales s
    WHERE s.orderdate BETWEEN '2010-10-01' AND '2010-12-31'
    group by s.productkey
)
select
s1.productkey,
COALESCE(s1.totalsellersales, 0::money) totalsellersales,
COALESCE(s2.totalinternetsales, 0::money) totalinternetsales,
COALESCE(COALESCE(s1.totalsellersales, 0::money) + COALESCE(s2.totalinternetsales, 0::money), 0::money) totalextendedamount,
d.englishproductname
from sellersales s1
LEFT JOIN internetsales s2 ON s1.productkey = s2.productkey
LEFT JOIN dimproduct d ON d.productkey = s1.productkey;
"""

# 2-ой запрос
query2 = """
SELECT DISTINCT c.firstname, c.middlename, c.lastname
FROM factinternetsales s
LEFT JOIN dimcustomer c ON c.customerkey = s.customerkey
WHERE s.orderdate BETWEEN '2013-01-01' AND '2013-06-30';
"""

# 3-ий запрос
# P.S. Либо я что-то сделал неправильно, либо за указанный интервал времени не было продавца, который сделал больше 1к продаж ¯\_(ツ)_/¯
query3 = """
WITH sales AS (
	SELECT s.resellerkey, SUM(orderquantity) totalquantity
	FROM factresellersales s
	WHERE s.orderdate BETWEEN '2012-04-01' AND '2012-06-30'
	GROUP BY s.resellerkey
)
SELECT r.resellername
from sales s
LEFT JOIN dimreseller r ON s.resellerkey = r.resellerkey
WHERE s.totalquantity > 1000;
"""

# 4-ый запрос
query4 = """
WITH products AS (
    SELECT p.productkey, sc.productcategorykey
    FROM dimproductsubcategory sc
    INNER JOIN dimproduct p ON p.productsubcategorykey = sc.productsubcategoryalternatekey
    WHERE sc.productcategorykey = 1
),
resellersales AS (
    SELECT p.productcategorykey, COALESCE(SUM(s.extendedamount), 0::money) totalsellersales
    FROM products p
    INNER JOIN factresellersales s ON s.productkey = p.productkey
    GROUP BY p.productcategorykey
),
internetsales AS (
    SELECT p.productcategorykey, COALESCE(SUM(s.extendedamount), 0::money) totalinternetsales
    FROM products p
    INNER JOIN factinternetsales s ON s.productkey = p.productkey
    GROUP BY p.productcategorykey
)
SELECT
    s1.productcategorykey,
    s1.totalsellersales,
    s2.totalinternetsales,
    (COALESCE(s1.totalsellersales, 0::money) + COALESCE(s2.totalinternetsales, 0::money)) totalextendedamount
FROM resellersales s1
LEFT JOIN internetsales s2 ON s1.productcategorykey = s2.productcategorykey;
"""

def combine_names(first_name, middle_name, last_name):
    full_name = f"{first_name} {middle_name} {last_name}" if middle_name else f"{first_name} {last_name}"
        
    return full_name

def read_query1():
    df = pd.read_sql_query(query1, engine)
    df_dict = df.to_dict(orient='records')
    
    content = {}
    for item in df_dict:
        content[item["englishproductname"]] = item["totalextendedamount"]
    
    return {"query1": content}

def read_query2():
    df = pd.read_sql_query(query2, engine)
    df_dict = df.to_dict(orient='records')
    
    content = []
    for item in df_dict:
        content.append(combine_names(item["firstname"], item["middlename"], item["lastname"]))
        
    return  {"query2": content}

def read_query3():
    df = pd.read_sql_query(query3, engine)
    df_dict = df.to_dict(orient='records')
    
    content = []
    for item in df_dict:
        content.append(item["resellername"])
    
    return {"query3": content}

def read_query4():
    df = pd.read_sql_query(query4, engine)
    df_dict = df.to_dict(orient='records')
    
    try:
        data = df_dict[0]
    except IndexError:
        data = {}
    
    content = {
        "category": data.get("productcategorykey", None),
        "total_amount": data.get("totalextendedamount", 0)
    }
    
    return {"query4": content}

if __name__ == "__main__":
    try:
        query1_content = read_query1()
        query2_content = read_query2()
        query3_content = read_query3()
        query4_content = read_query4()
        
        content = {
            **query1_content,
            **query2_content,
            **query3_content,
            **query4_content
        }
        
        with open("output.json", "w") as f:
            f.write(json.dumps(content, indent=4))
    except Exception as e:
        print(f"Error: {e}")
