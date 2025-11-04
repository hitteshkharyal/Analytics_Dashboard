

import sqlite3
from sqlite3 import Connection
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

DB_PATH = "shop.db"

# --------------------------
# Database helpers
# --------------------------
def get_conn(path=DB_PATH) -> Connection:
    conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn: Connection):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        cost_price REAL NOT NULL,
        selling_price REAL NOT NULL,
        stock_qty INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_time TIMESTAMP NOT NULL,
        customer_name TEXT,
        total_amount REAL
    );

    CREATE TABLE IF NOT EXISTS order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        qty INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    );
    """)
    conn.commit()

def seed_dummy_data(conn: Connection, force=False):
    cur = conn.cursor()
    cur.execute("SELECT count(*) as c FROM products")
    if cur.fetchone()["c"] > 0 and not force:
        return

    # Clear old data
    cur.executescript("DELETE FROM order_items; DELETE FROM orders; DELETE FROM products;")
    conn.commit()

    # Insert products
    products = [
        ("Milk 1L", 20.0, 25.0, 50),
        ("Bread Loaf", 15.0, 20.0, 30),
        ("Eggs Pack (12)", 60.0, 75.0, 20),
        ("Toothpaste", 40.0, 55.0, 10),
        ("Soap", 10.0, 20.0, 5),
        ("Rice 5kg", 250.0, 300.0, 8),
        ("Cooking Oil 1L", 120.0, 150.0, 12),
        ("Sugar 1kg", 40.0, 50.0, 0),
        ("Salt 1kg", 12.0, 20.0, 40),
        ("Tea Pack 100g", 30.0, 45.0, 3),
    ]
    for name, cost, sell, stock in products:
        cur.execute("INSERT INTO products (name, cost_price, selling_price, stock_qty) VALUES (?, ?, ?, ?)",
                    (name, cost, sell, stock))
    conn.commit()

    # Create dummy orders for past 30 days
    now = datetime.now()
    sample_orders = [
        ("Amit", now - timedelta(days=2), [(1, 2), (2, 1)]),
        ("Shreya", now - timedelta(days=5), [(3, 1), (5, 2)]),
        ("Ramesh", now - timedelta(days=10), [(6, 1), (7, 1)]),
        ("Priya", now - timedelta(days=1), [(1, 1), (2, 2), (9, 1)]),
        ("Karan", now - timedelta(days=4), [(10, 1), (4, 1)]),
        ("Neha", now - timedelta(days=8), [(1, 1), (8, 1)]),
        ("LocalShop", now - timedelta(days=6), [(9, 5), (2, 5)]),
    ]
    for cust, otime, items in sample_orders:
        total = 0.0
        cur.execute("INSERT INTO orders (order_time, customer_name, total_amount) VALUES (?, ?, ?)", (otime, cust, 0.0))
        order_id = cur.lastrowid
        for pid, qty in items:
            cur.execute("SELECT selling_price FROM products WHERE id = ?", (pid,))
            row = cur.fetchone()
            if not row:
                continue
            unit = row["selling_price"]
            cur.execute("INSERT INTO order_items (order_id, product_id, qty, unit_price) VALUES (?, ?, ?, ?)",
                        (order_id, pid, qty, unit))
            total += unit * qty
            cur.execute("UPDATE products SET stock_qty = CASE WHEN stock_qty - ? < 0 THEN 0 ELSE stock_qty - ? END WHERE id = ?",
                        (qty, qty, pid))
        cur.execute("UPDATE orders SET total_amount = ? WHERE id = ?", (total, order_id))
    conn.commit()

# --------------------------
# Analytics
# --------------------------
def load_tables(conn: Connection):
    products = pd.read_sql_query("SELECT * FROM products", conn)
    orders = pd.read_sql_query("SELECT * FROM orders", conn, parse_dates=["order_time"])
    order_items = pd.read_sql_query("SELECT oi.*, p.name as product_name, p.cost_price FROM order_items oi JOIN products p ON oi.product_id=p.id", conn)
    return products, orders, order_items

def compute_best_selling(order_items):
    if order_items.empty:
        return pd.DataFrame()
    summary = order_items.groupby('product_id', as_index=False).agg({'qty':'sum'})
    summary = summary.merge(order_items[['product_id','product_name']].drop_duplicates(), on='product_id', how='left')
    return summary.sort_values('qty', ascending=False).head(5)

def compute_top_profit_products(order_items):
    if order_items.empty:
        return pd.DataFrame()
    df = order_items.copy()
    df['profit'] = (df['unit_price'] - df['cost_price']) * df['qty']
    profit_summary = df.groupby(['product_id','product_name'], as_index=False)['profit'].sum()
    return profit_summary.sort_values('profit', ascending=False).head(5)

def compute_customers_per_hour(orders):
    if orders.empty:
        return pd.DataFrame()
    orders['hour'] = orders['order_time'].dt.hour
    per_hour = orders.groupby('hour', as_index=False)['id'].count().rename(columns={'id':'customers'})
    full = pd.DataFrame({'hour': range(0, 24)})
    return full.merge(per_hour, on='hour', how='left').fillna({'customers':0})

def compute_low_stock(products, order_items, threshold=2):
    low = products[products['stock_qty'] < threshold].copy()
    if low.empty:
        return low
    # Past 30-day sales
    last_30 = datetime.now() - timedelta(days=30)
    recent_sales = order_items[order_items['id'].isin(order_items.index)]
    recent_sales = recent_sales[recent_sales['id']>0] # dummy safety
    order_recent = pd.read_sql_query("SELECT * FROM orders", get_conn(), parse_dates=['order_time'])
    order_recent = order_recent[order_recent['order_time'] >= last_30]
    if not order_recent.empty:
        recent_ids = order_recent['id'].tolist()
        sales_30d = order_items[order_items['order_id'].isin(recent_ids)].groupby('product_id', as_index=False)['qty'].sum()
        low = low.merge(sales_30d, left_on='id', right_on='product_id', how='left').fillna({'qty':0})
        low.rename(columns={'qty':'sold_last_30d'}, inplace=True)
    else:
        low['sold_last_30d'] = 0
    return low[['id','name','stock_qty','sold_last_30d']]

# --------------------------
# Streamlit UI
# --------------------------
st.set_page_config(page_title="Daily Shop Dashboard", layout="wide")
st.title("🛒 Daily-Needs Shop Analytics Dashboard")

conn = get_conn()
init_db(conn)
seed_dummy_data(conn)

mode = st.sidebar.selectbox("Select Mode", ["Dashboard", "Admin - Manage Products", "Admin - Add Order","Power BI Dashboard"])

products_df, orders_df, order_items_df = load_tables(conn)

# --------------------------
# Dashboard
# --------------------------
if mode == "Dashboard":
    st.header("📊 Business Insights")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🔥 Best Selling Products")
        best = compute_best_selling(order_items_df)
        st.dataframe(best.rename(columns={'qty':'Units Sold'}))

        st.subheader("💰 Top Products by Profit")
        profit_df = compute_top_profit_products(order_items_df)
        st.dataframe(profit_df)

    with col2:
        st.subheader("⏰ Customers per Hour")
        per_hour = compute_customers_per_hour(orders_df)
        fig, ax = plt.subplots(figsize=(6,3))
        ax.bar(per_hour['hour'], per_hour['customers'])
        ax.set_xlabel("Hour of Day")
        ax.set_ylabel("Customers")
        st.pyplot(fig)

        st.subheader("⚠️ Low Stock Products (<2)")
        low_df = compute_low_stock(products_df, order_items_df)
        if low_df.empty:
            st.success("No low stock products.")
        else:
            st.dataframe(low_df)

elif mode == "Power BI Dashboard":
    st.title("📊 Power BI Business Analytics Dashboard")

    st.markdown("""
        <iframe title="ba_final" width="100%" height="600"
        src="https://app.powerbi.com/reportEmbed?reportId=efd8456f-36fa-4e91-ae0c-be2855468d00&autoAuth=true&ctid=34bd8bed-2ac1-41ae-9f08-4e0a3f11706c"
        frameborder="0" allowFullScreen="true"></iframe>
    """, unsafe_allow_html=True)

    st.info("This Power BI dashboard is connected to the same SQL data used in the Streamlit app.")
# --------------------------
# Admin - Manage Products
# --------------------------
elif mode == "Admin - Manage Products":
    st.header("🧑‍💼 Manage Products")

    st.subheader("Current Products List")
    st.dataframe(products_df)

    st.markdown("### ➕ Add New Product")
    with st.form("add_prod"):
        pname = st.text_input("Product name")
        cost = st.number_input("Cost price (₹)", min_value=0.0)
        sell = st.number_input("Selling price (₹)", min_value=0.0)
        stock = st.number_input("Initial stock", min_value=0, step=1)
        if st.form_submit_button("Add Product"):
            cur = conn.cursor()
            cur.execute("INSERT INTO products (name, cost_price, selling_price, stock_qty) VALUES (?, ?, ?, ?)",
                        (pname, cost, sell, stock))
            conn.commit()
            st.success("Product added successfully.")
            st.experimental_rerun()

    st.markdown("### 🔄 Add Stock to Existing Product")
    with st.form("add_stock"):
        pid = st.number_input("Enter Product ID", min_value=1, step=1)
        add_qty = st.number_input("Add Quantity", min_value=1, step=1)
        if st.form_submit_button("Update Stock"):
            cur = conn.cursor()
            cur.execute("UPDATE products SET stock_qty = stock_qty + ? WHERE id = ?", (add_qty, pid))
            conn.commit()
            st.success(f"Added {add_qty} units to Product ID {pid}.")
            st.rerun()

# --------------------------
# Admin - Add Order
# --------------------------
elif mode == "Admin - Add Order":
    st.header("🧾 Create Order & Generate Bill")

    product_choices = {f"{r['id']}: {r['name']} (₹{r['selling_price']}, stock {r['stock_qty']})": r['id'] for _,r in products_df.iterrows()}
    if not product_choices:
        st.info("No products found. Add products first.")
    else:
        with st.form("new_order"):
            cust = st.text_input("Customer name", "Walk-in")
            order_time = datetime.now()
            if 'order_items_tmp' not in st.session_state:
                st.session_state['order_items_tmp'] = []
            psel = st.selectbox("Select Product", list(product_choices.keys()))
            qty = st.number_input("Quantity", min_value=1, value=1)
            if st.form_submit_button("Add Item"):
                st.session_state['order_items_tmp'].append({'product_id': product_choices[psel], 'qty': qty})
                st.experimental_rerun()

        st.subheader("Current Items")
        lines = []
        for item in st.session_state['order_items_tmp']:
            p = products_df[products_df['id']==item['product_id']].iloc[0]
            lines.append({
                'Product': p['name'],
                'Qty': item['qty'],
                'Unit Price': p['selling_price'],
                'Line Total': p['selling_price']*item['qty']
            })
        if lines:
            bill_df = pd.DataFrame(lines)
            st.table(bill_df)
            total_amt = bill_df['Line Total'].sum()
            if st.button("Finalize Order"):
                cur = conn.cursor()
                cur.execute("INSERT INTO orders (order_time, customer_name, total_amount) VALUES (?, ?, ?)", (order_time, cust, total_amt))
                oid = cur.lastrowid
                for item in st.session_state['order_items_tmp']:
                    cur.execute("SELECT selling_price FROM products WHERE id=?", (item['product_id'],))
                    price = cur.fetchone()['selling_price']
                    cur.execute("INSERT INTO order_items (order_id, product_id, qty, unit_price) VALUES (?, ?, ?, ?)",
                                (oid, item['product_id'], item['qty'], price))
                    cur.execute("UPDATE products SET stock_qty = CASE WHEN stock_qty - ? < 0 THEN 0 ELSE stock_qty - ? END WHERE id = ?",
                                (item['qty'], item['qty'], item['product_id']))
                conn.commit()

                st.success(f"Order saved (ID {oid}). Total ₹{total_amt:.2f}")
                st.session_state['order_items_tmp'] = []
                st.experimental_rerun()
        else:
            st.info("Add products to order first.")
