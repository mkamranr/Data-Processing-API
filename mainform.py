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
from detailform import DetailForm


class MainForm:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Processing")
        #self.root.geometry("800x800")

        self.detail_form = None

        # Get the screen width and height
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()

        # Set the window's dimensions to match the screen's width and height
        self.root.geometry(f"{screen_width}x{screen_height}")

        self.root.state("zoomed")
        #self.root.resizable(False, True)

        self.frame = ttk.Frame(root)
        self.frame.pack(fill=tk.BOTH, expand=True)

        # Create a Treeview widget for the grid
        #self.tree = ttk.Treeview(self.frame, columns=(), show='headings', selectmode="browse")
        # Pack the Treeview widget
        #self.tree.pack(side="top", fill=tk.BOTH, expand=False)

        # Create a Menu widget
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

        self.tk = tk

        self.db = DBHelper()
        self.db.connect()
        self.load_data()

    def process_file_treeview(self):
        selected_item = self.tree.selection()  # Get the selected item ID
        if selected_item:
            # Retrieve the data for the selected item
            data = self.tree.item(selected_item)['values']
            print(f"Selected Item Data: {data}")

    def delete_file(self, data, row_num):
        print(f"Deleted Item Data: {data}")
        data_to_update = {
            "IsActive": 0
        }
        self.db.update_record("Process.DataFiles", data_to_update, "FileID=" + str(data[0]))
        self.clear_row(row_num)

    def process_file(self, data):
        #print(f"Selected Item Data: {data}")
        self.open_detail_form(data)

    def clear_row(self, row):
        print(row)
        # Remove all widgets in the specified row
        for widget in self.frame.grid_slaves(row=row):
            widget.grid_remove()

    def load_data(self):
        result, column_names = self.db.read_records("Process.DataFiles", "[FileID],[FileName],[FilePath],[RowsCount],[ColumnsCount],[CreatedBy],[CreatedOn],[StatusID]","IsActive=1")

        colIndex = 0
        # Create labels for column headers
        for col, column_name in enumerate(column_names):
            header_label = self.tk.Label(self.frame, text=column_name, relief=self.tk.GROOVE)
            header_label.grid(row=0, column=col, sticky=self.tk.NSEW)
            colIndex = col

        header_label = self.tk.Label(self.frame, text="Actions", relief=self.tk.GROOVE)
        header_label.grid(row=0, column=colIndex + 1, columnspan=2, sticky=self.tk.NSEW)

        # Fetch and display the data rows
        row_num = 1
        for row in result:
            col = 0
            for value in row:
                data_label = self.tk.Label(self.frame, text=value, relief=self.tk.GROOVE)
                data_label.grid(row=row_num, column=col, sticky=self.tk.NSEW)
                col = col + 1

            process_button = self.tk.Button(self.frame, text="Process", relief=self.tk.GROOVE, command=lambda row_value=row: self.process_file(row_value))
            process_button.grid(row=row_num, column=col, sticky=self.tk.NSEW)

            delete_button = self.tk.Button(self.frame, text="Delete", relief=self.tk.GROOVE, command=lambda row_index=row_num, row_value=row: self.delete_file(row_value, row_index))
            delete_button.grid(row=row_num, column=col + 1, sticky=self.tk.NSEW)

            row_num += 1

    def load_data_treeview(self):
        result, column_names = self.db.read_records("Process.DataFiles")

        # Clear existing data in the grid
        for i in self.tree.get_children():
            self.tree.delete(i)

        self.tree["columns"] = column_names

        for col in column_names:
            self.tree.column(col, anchor="w", width=100)
            self.tree.heading(col, text=col)

        i = 0
        for row in result:
            print(row)
            self.tree.insert("", i, values=tuple(row))
            i = i + 1

    def get_dataset_id(self):
        dataset_id_rows, column_names = self.db.read_records("Gen.SerialGenerator", "Value", "Attribute='DATASET_ID'")
        self.db.execute_query("update Gen.SerialGenerator set Value = Value + 1 where Attribute = 'DATASET_ID'")
        return dataset_id_rows[0][0]

    def open_detail_form(self, row_data):
        #print('open_detail_form')
        if self.detail_form is None:
            #print('if self.detail_form is None')
            detail_window = tk.Toplevel(self.root)
            self.detail_form = DetailForm(detail_window, row_data, self.close_detail_form)

    def close_detail_form(self):
        #print('close_detail_form')
        self.detail_form = None

    # Function to open a file
    def open_file(self):
        file_path = filedialog.askopenfilename(title="Open File")
        if file_path:
            # You can add code here to handle the opened file
            #print(f"Opened file: {file_path}")

            # Specify the destination directory and filename
            destination_dir = "datasets\\"

            # Create a subdirectory within the destination directory
            dataset_id = self.get_dataset_id()
            subdirectory_name = str(dataset_id)

            #print(subdirectory_name)

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
            #with open(file_path, 'rb') as file:
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

            #print(file_data)

            #file_binary = pyodbc.Binary(file_data)

            #print(file_binary)

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

            #print(my_dict)

            # Save the file data to the SQL Server database
            self.db.create_record("Process.DataFiles", my_dict)

            self.load_data()

if __name__ == "__main__":
    root = tk.Tk()
    app = MainForm(root)
    root.mainloop()