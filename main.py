# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

table = pd.read_sql("""SELECT * FROM sqlite_master""", conn)
#print(table)

employee_table = pd.read_sql("""SELECT * FROM employees""",conn)
#print(employee_table)
offices = pd.read_sql("""SELECT * FROM offices """,conn)
#print(offices.info(verbose=True))

orders = pd.read_sql("""SELECT * FROM orders """,conn)
#print(orders['orderNumber'].isna().unique())
#print(orders['status'].unique())
#print(orders.columns)
#print(orders['status'].unique())

products = pd.read_sql("""SELECT * FROM products""",conn)
print(products.columns)
order_details = pd.read_sql("""SELECT * FROM orderdetails""",conn)
print(order_details.columns)

customers = pd.read_sql("""SELECT * FROM customers""",conn)

#print(offices.columns)
# STEP 1
# Replace None with your code
df_boston = pd.read_sql("""
SELECT firstName, jobTitle
FROM employees
JOIN offices
    ON employees.officeCode = offices.officeCode
  WHERE offices.city = "Boston"
 """,conn)


# STEP 2
# Replace None with your code
df_zero_emp = pd.read_sql("""
SELECT offices.officeCode, COUNT(employees.employeeNumber) AS number_employees
FROM offices
  LEFT JOIN employees 
  USING(officeCode)
GROUP BY officeCode
HAVING number_employees = 0
""",conn)


# STEP 3
# Replace None with your code
df_employee = pd.read_sql("""
SELECT employees.firstName, employees.lastName, offices.city, offices.state
FROM employees
    LEFT JOIN offices
    USING(officeCode)   
ORDER BY employees.firstName, employees.lastname ASC               
""",conn)


# STEP 4
# Replace None with your code
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers AS c
  LEFT JOIN orders
  USING(customerNumber)
WHERE orders.customerNumber IS NULL
ORDER BY c.contactLastName ASC
 """,conn)

#print(df_contacts)

# STEP 5
# Replace None with your code
df_payment = pd.read_sql("""
SELECT customers.contactFirstName, customers.contactLastName, CAST(payments.amount AS FLOAT) as amount, payments.paymentDate
FROM customers
  JOIN payments
  USING(customerNumber)
ORDER BY amount DESC
 """,conn)

# STEP 6
# Replace None with your code
df_credit = pd.read_sql(""" 
SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(customers.customerNumber) AS num_customers
FROM employees AS e
  JOIN customers 
  ON e.employeeNumber = customers.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(customers.creditLimit) > 90000.00
ORDER BY num_customers DESC
""",conn)

# STEP 7
# Replace None with your code

df_product_sold = pd.read_sql("""
SELECT p.productName, COUNT(o.orderNumber) AS numorders, SUM(quantityOrdered) AS totalunits
FROM products AS p
  JOIN orderdetails AS o
   USING(productCode)
GROUP BY p.productName
ORDER BY totalunits DESC
""",conn)

print(df_product_sold)

# STEP 8
# Replace None with your code
df_total_customers = None

# STEP 9
# Replace None with your code
df_customers = None

# STEP 10
# Replace None with your code
df_under_20 = None

conn.close()