import pyodbc

class DBHelper:
    def __init__(self):
        connection_string = 'DRIVER={SQL Server};SERVER=DB1;DATABASE=DataProcessingAI;UID=dataprocessing;PWD=dataprocessing'
        self.connection_string = connection_string
        self.connection = None

    def connect(self):
        try:
            self.connection = pyodbc.connect(self.connection_string)
        except Exception as e:
            print(f"Connection error: {str(e)}")

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print(f"Disconnection error: {str(e)}")

    def execute_query(self, query, params=None):
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
        except Exception as e:
            print(f"Query execution error: {str(e)}")

    def create_record(self, table_name, data):
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = list(data.values())
        self.execute_query(query, values)

    def read_records(self, table_name, columns=None, condition=None):
        if not columns:
            columns = '*'
        query = f"SELECT {columns} FROM {table_name}"
        if condition:
            query += f" WHERE {condition}"
        cursor = self.connection.cursor()
        cursor.execute(query)

        # Fetch column names from the cursor description
        column_names = [desc[0] for desc in cursor.description]

        rows = cursor.fetchall()
        cursor.close()
        return rows, column_names

    def read_records_join(self, table_name, columns=None, joins=None, condition=None):
        if not columns:
            columns = '*'
        query = f"SELECT {columns} FROM {table_name}"

        if joins:
            query += f" {joins} "

        if condition:
            query += f" WHERE {condition}"
        cursor = self.connection.cursor()
        cursor.execute(query)

        rows = cursor.fetchall()
        cursor.close()
        return rows

    def update_record(self, table_name, data, condition):
        set_values = ', '.join([f"{key} = ?" for key in data.keys()])
        query = f"UPDATE {table_name} SET {set_values} WHERE {condition}"
        values = list(data.values())
        self.execute_query(query, values)

    def delete_record(self, table_name, condition):
        query = f"DELETE FROM {table_name} WHERE {condition}"
        self.execute_query(query)

