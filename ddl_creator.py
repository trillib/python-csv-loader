import csv

class DDLCreator:
    def __init__(self, database):
        self.database = database

    def create_table_from_file(self, file_name, table_name):
        with open(file_name, encoding="utf-8") as file:
            query = self._create_query_from_columns(
                self._get_columns_from_file(file), table_name)
            self.database.cursor().execute("DROP TABLE IF EXISTS {}".format(table_name))
            self.database.cursor().execute(query)
            print("Table {} successfully created".format(table_name))

    def _create_query_from_columns(self, columns, table_name):
        return "CREATE TABLE {table_name} ({columns})".format(
            table_name=table_name,
            columns=self._get_prepared_columns(columns)
        )

    def _get_columns_from_file(self, file):
        reader = csv.reader(file, delimiter=";")
        return next(reader)

    def _get_prepared_columns(self, columns):
        query = " ".join([column_name.replace(" ", "_").replace(
            "-", "_") + " VARCHAR(255)," for column_name in columns])
        return query[:-1]

    def check_types(self, file_name):
        print(chkcsv.check_csv_file(file_name))
