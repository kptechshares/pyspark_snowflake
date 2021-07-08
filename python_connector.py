import snowflake.connector
import os
import base64


def read_data(cs):
    try:
        # cs.execute("USE DATABASE kp_database")
        # cs.execute("PUT file:///C:\\Users\\USER\\Documents\\Datasets\\Datasets\\movies\\u_user.csv @%u_user_table_stage")
        # con.cursor().execute("COPY INTO testtable")
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print("Printing Version of current SnowFlake",one_row[0])
    finally:
        cs.close()


def transform_data(cs):
    try:
        # cs.execute("USE DATABASE kp_database")
        # cs.execute("PUT file:///C:\\Users\\USER\\Documents\\Datasets\\Datasets\\movies\\u_user.csv @%u_user_table_stage")
        # con.cursor().execute("COPY INTO testtable")
        print("This block of code for transforming the data")
    finally:
        cs.close()

def write_data(cs):
    try:
        # cs.execute("USE DATABASE kp_database")
        # cs.execute("PUT file:///C:\\Users\\USER\\Documents\\Datasets\\Datasets\\movies\\u_user.csv @%u_user_table_stage")
        # con.cursor().execute("COPY INTO testtable")
        print("This block of code for loading the data")
    finally:
        cs.close()


if __name__ == "__main__":
    # Gets the version
    ctx = snowflake.connector.connect(
        user='kptechshares',
        # password='Midhuna@123',
        # base64.urlsafe_b64encode('Midhuna@123'.encode('UTF-8')).decode('ascii')
        password=base64.urlsafe_b64decode('Encrypt your password and past here'.encode('UTF-8')).decode('ascii'),
        account='past your account name till the region, no need to have snowflakecomputing.com'
    )
    cs = ctx.cursor()
    read_data(cs)
    transform_data(cs)
    write_data(cs)
    ctx.close()