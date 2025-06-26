import tkinter as tk
from tkinter import ttk
import sys
from tkinter import filedialog
from DBHelper import DBHelper
import datetime
import pyodbc
import binascii
import shutil
import os
import pandas as pd
from FileDetails import FileDetails
from FeatureSelection import FeatureSelection
from tkinter import messagebox


class Home(tk.Frame):
    def __init__(self, root):
        super(Home, self).__init__()
        self.root = root
        root.title("Data Processing")
        self.detail_form = None
        self.feature_selection_form = None

        # Calculate the center position for the Main Form
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        # x = (screen_width - 400) // 2
        # y = (screen_height - 300) // 2
        # root.geometry(f"400x300+{x}+{y}")

        root.geometry(f"{screen_width}x{screen_height}")
        root.state("zoomed")

        self.frame = ttk.Frame(root)
        self.frame.pack(fill=tk.BOTH, expand=True)

        # self.label = tk.Label(root, text="Welcome to the Main Form")
        # self.label.pack()
        #
        # self.entry = tk.Entry(root)
        # self.entry.pack()
        #
        # self.submit_button = tk.Button(root, text="Submit", command=self.submit)
        # self.submit_button.pack()
        #
        # self.open_child_button = tk.Button(root, text="Open Child Form", command=self.open_child_form)
        # self.open_child_button.pack()

        self.menu = tk.Menu(root)
        root.config(menu=self.menu)

        # Create a File menu
        self.file_menu = tk.Menu(self.menu, tearoff=0)
        self.menu.add_cascade(label="File", menu=self.file_menu)

        # Add File menu items
        self.file_menu.add_command(label="Upload", command=self.open_file)
        #        self.file_menu.add_command(label="Process Selected File", command=self.process_file_treeview)
        self.file_menu.add_separator()
        self.file_menu.add_command(label="Exit", command=root.quit)

        self.db = DBHelper()
        self.db.connect()
        self.load_data()

    def delete_file(self, data, row_num):
        print(f"Deleted Item Data: {data}")
        data_to_update = {
            "IsActive": 0
        }
        self.db.update_record("Process.DataFiles", data_to_update, "FileID=" + str(data[0]))
        self.clear_row(row_num)

    def process_file(self, data):
        # print(f"Selected Item Data: {data}")
        self.open_detail_form(data)

    def feature_selection(self, data):
        # print(f"Selected Item Data: {data}")
        self.open_feature_selection_form(data)

    def clear_row(self, row):
        print(row)
        # Remove all widgets in the specified row
        for widget in self.frame.grid_slaves(row=row):
            widget.grid_remove()

    def load_data(self):
        result, column_names = self.db.read_records("Process.DataFiles",
                                                    "[FileID],[FileName],[FilePath],[RowsCount],[ColumnsCount],[CreatedBy],[CreatedOn],[StatusID]",
                                                    "IsActive=1")

        colIndex = 0
        # Create labels for column headers
        for col, column_name in enumerate(column_names):
            header_label = tk.Label(self.frame, text=column_name, relief=tk.GROOVE)
            header_label.grid(row=0, column=col, sticky=tk.NSEW)
            colIndex = col

        header_label = tk.Label(self.frame, text="Actions", relief=tk.GROOVE)
        header_label.grid(row=0, column=colIndex + 1, columnspan=3, sticky=tk.NSEW)

        # Fetch and display the data rows
        row_num = 1
        for row in result:
            col = 0
            for value in row:
                data_label = tk.Label(self.frame, text=value, relief=tk.GROOVE)
                data_label.grid(row=row_num, column=col, sticky=tk.NSEW)
                col = col + 1

            process_button = tk.Button(self.frame, text="Data Cleaning", relief=tk.GROOVE,
                                       command=lambda row_value=row: self.process_file(row_value))
            process_button.grid(row=row_num, column=col, sticky=tk.NSEW)

            if row[7] == 2:
                features_button = tk.Button(self.frame, text="Feature Selection", relief=tk.GROOVE,
                                            command=lambda row_value=row: self.feature_selection(row_value))
                features_button.grid(row=row_num, column=col + 1, sticky=tk.NSEW)

            delete_button = tk.Button(self.frame, text="Delete", relief=tk.GROOVE,
                                      command=lambda row_index=row_num, row_value=row: self.delete_file(row_value,
                                                                                                        row_index))
            delete_button.grid(row=row_num, column=col + 2, sticky=tk.NSEW)

            row_num += 1

    def submit(self):
        input_text = self.entry.get()
        if input_text:
            self.label.config(text=f"You entered: {input_text}")

    def open_child_form(self):
        child_window = tk.Toplevel(self.root)
        child_form = FileDetails(child_window)
        child_form.focus_child_form()

    def get_dataset_id(self):
        dataset_id_rows, column_names = self.db.read_records("Gen.SerialGenerator", "Value",
                                                             "Attribute='DATASET_ID'")
        self.db.execute_query("update Gen.SerialGenerator set Value = Value + 1 where Attribute = 'DATASET_ID'")
        return dataset_id_rows[0][0]

    def open_detail_form(self, row_data):
        if self.detail_form is None:
            # detail_window = tk.Toplevel(self.root)
            # self.detail_form = DetailForm(detail_window, row_data, self.close_detail_form)
            detail_window = tk.Toplevel(self.root)
            child_form = FileDetails(detail_window, row_data, self.close_detail_form)
            child_form.focus_child_form()

    def close_detail_form(self):
        # print('close_detail_form')
        self.detail_form = None
        self.clear_grid_layout()
        self.load_data()

    def open_feature_selection_form(self, row_data):
        if self.feature_selection_form is None:
            # detail_window = tk.Toplevel(self.root)
            # self.detail_form = DetailForm(detail_window, row_data, self.close_detail_form)
            feature_selection_window = tk.Toplevel(self.root)
            child_form = FeatureSelection(feature_selection_window, row_data, self.close_feature_selection_form)
            child_form.focus_child_form()

    def close_feature_selection_form(self):
        # print('close_detail_form')
        self.feature_selection_form = None
        self.clear_grid_layout()
        self.load_data()

    def clear_grid_layout(self):
        # Get a list of all widgets in the grid
        widgets = self.frame.winfo_children()

        # Destroy each widget to clear the grid
        for widget in widgets:
            widget.destroy()

    # Function to open a file
    def open_file(self):
        file_path = filedialog.askopenfilename(title="Select a CSV File",
                                               filetypes=[("CSV Files", "*.csv")])
        if file_path and file_path.endswith(".csv"):
            # You can add code here to handle the opened file
            # print(f"Opened file: {file_path}")

            # Specify the destination directory and filename
            destination_dir = "datasets\\"

            # Create a subdirectory within the destination directory
            dataset_id = self.get_dataset_id()
            subdirectory_name = str(dataset_id)

            # print(subdirectory_name)

            subdirectory_path = os.path.join(destination_dir, subdirectory_name)
            os.makedirs(subdirectory_path, exist_ok=True)

            # Specify the destination file path in the subdirectory
            destination_file = os.path.join(subdirectory_path, file_path.split("/")[-1])

            try:
                # Copy the selected file to the destination
                shutil.copy(file_path, destination_file)
                print(f"File saved to {destination_file}")
            except Exception as e:
                print(f"Error saving the file: {str(e)}")

            # Open the file, read its content, and convert it to bytes
            # with open(file_path, 'rb') as file:
            #                file_data = file.read()
            #                file_data_hex = '0x'.encode('ascii') + binascii.hexlify(file_data)

            # Extract the file name and extension from the file path
            file_name = file_path.split("/")[-1]  # For Unix-like systems
            # file_name = file_path.split("\\")[-1]  # For Windows
            file_name_parts = file_name.split(".")

            file_size = os.path.getsize(file_path)

            file_extension = ""

            if len(file_name_parts) > 1:
                file_extension = file_name_parts[-1]

            # Get the current date and time
            current_datetime = datetime.datetime.now()

            # Format the date and time as a string in a suitable format for SQL Server
            formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

            # print(file_data)

            # file_binary = pyodbc.Binary(file_data)

            # print(file_binary)

            dataset = pd.read_csv(file_path)

            my_dict = {
                "FileID": dataset_id,
                "FileName": file_name,
                "FileExtenstion": file_extension,
                "FilePath": destination_file,
                "FileSize": file_size,
                "RowsCount": dataset.shape[0],
                "ColumnsCount": dataset.shape[1],
                "CreatedBy": 1,
                "CreatedOn": formatted_datetime,
                "StatusID": 1
            }

            # print(my_dict)

            # Save the file data to the SQL Server database
            self.db.create_record("Process.DataFiles", my_dict)

            self.db.execute_query("EXEC Process.SaveDataFilesTasks ?", dataset_id)

            self.load_data()
        else:
            messagebox.showerror("Error", "Invalid file selected. Please upload a CSV file.")


if __name__ == "__main__":
    root = tk.Tk()
    app = Home(root)
    root.mainloop()
