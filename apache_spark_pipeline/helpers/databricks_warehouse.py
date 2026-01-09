# import databricks.sql as dbsql
# import pandas as pd
# # Connect to your Databricks SQL warehouse
# connection = dbsql.connect(
#                         server_hostname = "",
#                         http_path = "",
#                         access_token = "")

# cursor = connection.cursor()

# cursor.execute("CREATE SCHEMA IF NOT EXISTS main")

# tables_sql = {
#     "users": """
#         CREATE TABLE IF NOT EXISTS main.users (
#             user_id STRING,
#             username STRING,
#             email STRING,
#             updated_at TIMESTAMP
#         ) USING DELTA
#     """,
#     "products": """
#         CREATE TABLE IF NOT EXISTS main.products (
#             product_id STRING,
#             name STRING,
#             price DOUBLE,
#             updated_at TIMESTAMP
#         ) USING DELTA
#     """,
#     "purchases": """
#         CREATE TABLE IF NOT EXISTS main.purchases (
#             user_id STRING,
#             product_id STRING,
#             amount DOUBLE,
#             updated_at TIMESTAMP,
#             ordered_at TIMESTAMP
#         ) USING DELTA
#     """
# }

# for name, sql_stmt in tables_sql.items():
#     cursor.execute(sql_stmt)


# users_df = pd.DataFrame({
#     "user_id": ["u1", "u2", "u3"],
#     "username": ["alice", "bob", "carol"],
#     "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
#     "updated_at": pd.Timestamp("2026-01-08 22:00:00")
# })

# products_df = pd.DataFrame({
#     "product_id": ["p1", "p2", "p3"],
#     "name": ["Laptop", "Phone", "Tablet"],
#     "price": [1200.0, 800.0, 500.0],
#     "updated_at": pd.Timestamp("2026-01-08 22:00:00")
# })

# purchases_df = pd.DataFrame({
#     "user_id": ["u1", "u2", "u3"],
#     "product_id": ["p2", "p3", "p1"],
#     "amount": [800.0, 500.0, 1200.0],
#     "updated_at": pd.Timestamp("2026-01-08 22:05:00"),
#     "ordered_at": pd.Timestamp("2026-01-08 22:00:00")
# })

# print("Mock datasets ready.")

# def insert_dataframe_param(df, table_name):
#     columns = df.columns.tolist()
#     placeholders = ", ".join(["?"] * len(columns))
#     insert_sql = f"INSERT INTO main.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"

#     for _, row in df.iterrows():
#         cursor.execute(insert_sql, tuple(row))

# insert_dataframe_param(users_df, "users")
# insert_dataframe_param(products_df, "products")
# insert_dataframe_param(purchases_df, "purchases")
# # -----------------------------
# # Step 6: Fetch tables into Pandas for TigerGraph pipeline
# # -----------------------------
# def fetch_table(table_name):
#     cursor.execute(f"SELECT * FROM main.{table_name}")
#     rows = cursor.fetchall()
#     columns = [col[0] for col in cursor.description]
#     return pd.DataFrame(rows, columns=columns)

# df_users = fetch_table("users")
# df_products = fetch_table("products")
# df_purchases = fetch_table("purchases")

# print(df_users.head())

# print(df_products.head())

# print(df_purchases.head())

# cursor.close()
# connection.close()