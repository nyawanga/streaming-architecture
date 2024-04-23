from faker import Faker
import psycopg2
from psycopg2.extras import execute_values

# Initialize Faker to generate fake data
fake = Faker()

# Database connection parameters
conn_params = (
    "dbname='franco' user='franco' host='localhost' password='franco' port='5433'"
)
conn = psycopg2.connect(conn_params)
cur = conn.cursor()


def generate_customers(n):
    customers = []
    for _ in range(n):
        customers.append(
            (
                fake.last_name()[:50],
                fake.street_address()[:50],
                fake.street_address()[:50],
                fake.date_of_birth().isoformat()[:50],
                str(fake.random_int(min=18, max=99))[:50],
                f"{fake.random_int(min=1, max=50)} miles"[:50],
                fake.uuid4()[:50],
                fake.uuid4()[:50],
                fake.date_this_decade().isoformat()[:50],
                fake.email()[:50],
                fake.word()[:50],
                fake.job()[:50],
                fake.word()[:50],
                fake.first_name()[:50],
                fake.job()[:50],
                fake.random_element(["M", "F"])[:50],
                fake.random_element(["Y", "N"])[:50],
                fake.random_element(["S", "M"])[:50],
                fake.first_name()[:50],
                "Formal"[:50],
                str(fake.random_int(min=0, max=5))[:50],
                str(fake.random_int(min=0, max=3))[:50],
                fake.phone_number()[:50],
                fake.word()[:50],
                fake.job()[:50],
                fake.suffix()[:50],
                fake.prefix()[:50],
                str(fake.random_int(min=0, max=3))[:50],
                str(fake.random_int(min=20000, max=200000))[:50],
            )
        )
    return customers


def generate_sales_territories(n):
    territories = []
    for _ in range(n):
        territories.append((fake.country()[:50], fake.state()[:50], fake.city()[:50]))
    return territories


def generate_employees(n):
    employees = []
    for _ in range(n):
        employees.append((fake.name()[:50], fake.state()[:50]))
    return employees


def generate_sales(n, max_id, customer_ids, employee_ids, territory_ids):
    sales = []
    if max_id is None:
        max_id = 0
    end = max_id + n
    for sales_id in range(max_id + 1, end, 1):
        sales.append(
            (
                sales_id,
                # str(fake.random_element(customer_ids)),
                str(fake.random_element(employee_ids)),
                str(fake.random_element(territory_ids)),
                str(fake.random_number(digits=3))[:50],
                fake.date_this_decade().isoformat()[:50],
                fake.date_this_decade().isoformat()[:50],
                str(fake.random_number(digits=5))[:50],
                str(fake.random_number(digits=2))[:50],
                fake.date_this_decade().isoformat()[:50],
                str(fake.random_int(min=1, max=100))[:50],
                str(fake.random_number(digits=2))[:50],
                str(fake.random_number(digits=2))[:50],
                str(fake.random_number(digits=3))[:50],
                str(fake.random_int(min=1, max=100))[:50],
                str(fake.random_int(min=1000, max=9999))[:50],
                str(fake.random_number(digits=2))[:50],
                fake.date_this_decade().isoformat()[:50],
                str(fake.random_number(digits=2))[:50],
                str(fake.random_number(digits=5))[:50],
                str(fake.random_number(digits=2))[:50],
                str(fake.random_number(digits=2))[:50],
                str(fake.random_element(employee_ids)),
            )
        )
    return sales


# Insert data into the database
def insert_data(table_name, rows, columns):
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    execute_values(cur, sql, rows)
    conn.commit()


def get_table_columns(table_name: str, schema: str):
    cur.execute(
        f"""
        SELECT *
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name   = '{table_name}';
        """
    )
    result = cur.fetchall()
    return [row[3] for row in result]


# Generate and insert data
customer_data = generate_customers(100)
customer_columns = get_table_columns("customer", "franco")
customer_columns.remove("customer_id")
insert_data("franco.customer", customer_data, customer_columns)

territory_data = generate_sales_territories(10)
territory_columns = get_table_columns("sales_territory", "franco")
territory_columns.remove("sales_territory_id")
insert_data("franco.sales_territory", territory_data, territory_columns)

employee_data = generate_employees(15)
employee_columns = get_table_columns("employee", "franco")
employee_columns.remove("employee_id")
insert_data("franco.employee", employee_data, employee_columns)

# Fetch IDs to ensure foreign key constraints are met
cur.execute("SELECT customer_id FROM franco.customer")
customer_ids = [row[0] for row in cur.fetchall()]

cur.execute("SELECT sales_territory_id FROM franco.sales_territory")
territory_ids = [row[0] for row in cur.fetchall()]

cur.execute("SELECT employee_id FROM franco.employee")
employee_ids = [row[0] for row in cur.fetchall()]

cur.execute("SELECT MAX(sales_id) FROM franco.sales")
max_sales_id = [row[0] for row in cur.fetchall()][0]

print(max_sales_id)

sales_data = generate_sales(
    500, max_sales_id, customer_ids, employee_ids, territory_ids
)
sales_columns = get_table_columns("sales", "franco")
insert_data("franco.sales", sales_data, sales_columns)

# # Close the connection
cur.close()
conn.close()
