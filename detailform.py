import tkinter as tk
from tkinter import ttk
import shutil
import os
import pandas as pd
from tkinter.messagebox import askyesno

class DetailForm:
    def __init__(self, root, data, close_callback):
        self.root = root
        self.data = data
        self.dataset = None
        self.root.title("Detail Form")

        self.root.geometry("800x800")

        self.frame = ttk.Frame(root)
        self.frame.pack(fill=tk.BOTH, expand=True)

        self.close_callback = close_callback

        #self.label = tk.Label(root, text=f"This is the Detail Form: {data}")
        #self.label.pack(pady=10)

        self.tk = tk

        self.close_button = tk.Button(root, text="Close", command=self.close)
        self.close_button.pack(pady=10)



        #print(f"Selected Item Data: {data}")

        root.protocol("WM_DELETE_WINDOW", self.close)

        self.load_dataset()

    def load_dataset(self):
        self.dataset = pd.read_csv(self.data[2])
        self.analyze_dataset()

    # click event handler
    def confirm_action(self, message):
        answer = askyesno(title='Confirmation', message=message)
        return answer

    # Function to display the DataFrame in a child window
    def display_dataframe(self, df):
        # Create a new child window
        child_window = tk.Toplevel(self.root)
        child_window.title('DataFrame Viewer')

        df = df.head(100)

        # Create a Pandas DataFrame as a Table
        tree = ttk.Treeview(child_window, columns=list(df.columns), show='headings')

        # Define the columns
        for col in df.columns:
            tree.heading(col, text=col)
            tree.column(col, width=150)  # Adjust the width as needed

        # Insert data into the treeview
        for i in range(len(df)):
            values = [df[col][i] for col in df.columns]
            tree.insert('', 'end', values=values)

        tree.pack()

    def analyze_dataset(self):
        rows = self.dataset.shape[0]
        cols = self.dataset.shape[1]

        print(self.dataset.shape)

        # row_label = self.tk.Label(self.frame, text=f"Dataset as {rows} number of rows", relief=self.tk.GROOVE)
        # row_label.grid(row=0, column=5, sticky=self.tk.NSEW)
        #
        # col_label = self.tk.Label(self.frame, text=f"Dataset as {cols} number of columns", relief=self.tk.GROOVE)
        # col_label.grid(row=5, column=5, sticky=self.tk.NSEW)

        # rows_entry_text = tk.StringVar()
        # cols_entry_text = tk.StringVar()
        #
        # rows_entry_text.set(str(rows))
        # cols_entry_text.set(str(cols))

        lblRows = self.tk.Label(self.frame, text="Number of rows").place(x=40, y=60)
        lblCols = self.tk.Label(self.frame, text="Number of columns").place(x=40, y=100)
        txtRows = self.tk.Label(self.frame, text=str(rows), width=30).place(x=210, y=60)
        txtCols = self.tk.Label(self.frame, text=str(cols), width=30).place(x=210, y=100)

        dataset = self.dataset.copy()

        show_data_button = tk.Button(self.root, text="Show Data",
                                          command=lambda df=dataset: self.display_dataframe(dataset))
        show_data_button.pack(pady=10)

        empty_columns = dataset.columns[dataset.isnull().all()]  # get names of columns with all values empty

        if (empty_columns.size > 0):
            comma_separated_column_names = ', '.join(empty_columns)
            confirm_msg = 'There are ' + str(empty_columns.size) + ' columns with all values empty. Columns: ' + comma_separated_column_names
            action = self.confirm_action(confirm_msg)

            if action:
                dataset = dataset.dropna(axis=1, how='all')  # Drop columns where all values are empty or null

        # Get numeric columns (int64 and float64)
        numeric_columns = dataset.select_dtypes(include=['int64', 'float64']).columns.tolist()

        # Get categorical columns (object)
        categorical_columns = dataset.select_dtypes(include=['object']).columns.tolist()

        # Find numeric columns with missing values
        columns_with_missing_values = dataset[numeric_columns].columns[dataset[numeric_columns].isnull().any()].tolist()

    def close(self):
        self.root.destroy()
        self.close_callback()