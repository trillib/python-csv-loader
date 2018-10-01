import csv


class DMLCreator:
    def __init__(self, database):
        self.database = database

    def fill_data_into_table(self, file_name, table_name):
        with open(file_name, encoding="utf-8") as file:
            data = self._get_data_from_file(file)
            query = self._get_insert_query(table_name, len(next(data)))
            cursor = self.database.cursor()
            
            print("Started inserting into Table")
            for line in data:
                cursor.execute(query, line)
            self.database.commit()
            cursor.close()

    def _get_insert_query(self, table_name, num_fields):
        assert(num_fields > 0)
        placeholders = (num_fields-1) * "%s, " + "%s"
        query = ("insert into %s" % table_name) + \
            (" values (%s)" % placeholders)
        return query

    def _get_data_from_file(self, file):
        reader = csv.reader(file, delimiter=";")
        next(reader)  # Skip Header
        return reader
