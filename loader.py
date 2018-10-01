import mysql.connector
import configparser
import sys
from ddl_creator import DDLCreator
from dml_creator import DMLCreator


def run(file_name, table_name):
    print("starting")
    my_db = get_database()
    print("connected")

    # Creates the table
    ddl_creator = DDLCreator(my_db)
    ddl_creator.create_table_from_file(file_name, table_name)

    # Fill the data into table
    dml_creator = DMLCreator(my_db)
    dml_creator.fill_data_into_table(file_name, table_name)

    # Show the data
    mycursor = my_db.cursor()
    mycursor.execute("select * from {}".format(table_name))
    result = mycursor.fetchall()
    for x in result:
        print(x)


def get_database():
    config = configparser.ConfigParser()
    config.read("database_config.ini")

    mydb = mysql.connector.connect(
        host=config["DEFAULT"]["Endpoint"],
        user=config["DEFAULT"]["User"],
        passwd=config["DEFAULT"]["Password"],
        database=config["DEFAULT"]["Database"],
        auth_plugin="mysql_native_password"
    )

    return mydb


if __name__ == "__main__":
    try:
        file_name = sys.argv[1]
        table_name = sys.argv[2]
    except IndexError:
        print("Usage: loader.py <csv filename> <table name>")
        sys.exit(1)

    run(file_name, table_name)
