import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import pandas as pd
from DBHelper import DBHelper
import os
from MissingData import MissingData
from NegativeData import NegativeData
from sklearn.impute import KNNImputer
import numpy as np
from tkinter import scrolledtext
from sklearn.preprocessing import LabelEncoder
import pickle


class FileDetails(tk.Frame):
    def __init__(self, root, data, close_callback):
        super(FileDetails, self).__init__()
        self.dialog_result = None
        self.root = root
        self.data = data
        self.dataset = None
        self.data_tasks = None
        self.data_tasks_dict = None
        root.title("Data Statistics")
        self.missing_data_form = None
        self.negative_data_form = None
        root.grab_set()

        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()

        w = screen_width - 100
        h = screen_height - 50

        x = (screen_width - w) // 2
        y = (screen_height - h) // 2

        root.geometry(f"{w}x{h}+{x}+{y}")

        # self.label = tk.Label(root, text="This is the Child Form")
        # self.label.pack()
        #
        # self.close_button = tk.Button(root, text="Close", command=self.close)
        # self.close_button.pack()
        #
        # self.confirm_button = tk.Button(root, text="Show Confirmation Dialog", command=self.show_confirmation_dialog)
        # self.confirm_button.pack()

        self.frame = ttk.Frame(root)
        self.frame.pack(fill=tk.BOTH, expand=True)

        self.close_callback = close_callback

        root.protocol("WM_DELETE_WINDOW", self.close)

        self.db = DBHelper()
        self.db.connect()

        self.load_dataset()

    def update_data_tasks(self):
        self.data_tasks = self.db.read_records_join("[Process].[DataFilesTasks] DFT",
                                                    "DFT.[ID],TSK.Description,DFT.[FileID],DFT.[TaskID],DFT.[TaskStatus],DFT.[UpdatedOn]",
                                                    "INNER JOIN [Gen].[Tasks] TSK on TSK.TaskID = DFT.TaskID",
                                                    "DFT.FileID=" + str(self.data[0]))

        # Specify the columns you want to pivot
        columns_to_pivot = [3, 4]
        # Create a dictionary with key-value pairs
        pivot_dict = {self.data_tasks[i][columns_to_pivot[0]]: self.data_tasks[i][columns_to_pivot[1]] for i in
                      range(len(self.data_tasks))}
        self.data_tasks_dict = pivot_dict

    def load_dataset(self):
        self.dataset = pd.read_csv(self.data[2])
        self.update_data_tasks()
        dataset = self.dataset.copy()

        has_completed_tasks = any(row[4] == 1 for row in self.data_tasks)

        if has_completed_tasks:
            folder_name = 'datasets\\' + str(self.data[0]) + '\\cleaned'
            file_path = os.path.join(folder_name, self.data[1])
            if os.path.exists(file_path):
                dataset = pd.read_csv(file_path)
                self.analyze_dataset(dataset, True)
            else:
                self.analyze_dataset(dataset, False)
        else:
            self.analyze_dataset(dataset, False)

    def close(self):
        self.root.grab_release()
        self.root.destroy()
        self.close_callback()

    def show_confirmation_dialog(self):
        response = self.show_yes_no_dialog("Confirmation", "Are you sure you want to proceed?")
        if response:
            messagebox.showinfo("Confirmation", "You clicked 'Yes'")
        else:
            messagebox.showinfo("Confirmation", "You clicked 'No'")

    def show_yes_no_dialog(self, title, message):
        dialog = tk.Toplevel(self.root)
        dialog.title(title)
        dialog.focus()
        dialog.grab_set()

        # Calculate the center position for the dialog
        screen_width = dialog.winfo_screenwidth()
        screen_height = dialog.winfo_screenheight()
        x = (screen_width - 600) // 2
        y = (screen_height - 150) // 2

        dialog.geometry(f"600x150+{x}+{y}")

        label = tk.Label(dialog, text=message)
        label.pack()

        button_frame = tk.Frame(dialog)  # Create a frame for the buttons
        button_frame.pack()

        yes_button = tk.Button(button_frame, text="Yes", command=lambda: self.set_dialog_result(dialog, True))
        no_button = tk.Button(button_frame, text="No", command=lambda: self.set_dialog_result(dialog, False))

        yes_button.pack(side=tk.LEFT)  # Place buttons next to each other
        no_button.pack(side=tk.LEFT)

        # dialog.focus()

        dialog.wait_window(dialog)
        self.root.grab_set()  # Re-grab ChildForm
        return self.dialog_result

    def set_dialog_result(self, dialog, result):
        self.dialog_result = result
        dialog.grab_release()
        dialog.destroy()

    def focus_child_form(self):
        self.root.focus_set()

    def encode_categorical_columns(self, df):
        myLblEncoders = {}
        myLblEncoderColumns = {}
        colIndex = 0
        for col in df.select_dtypes(include=['O']).columns:
            myLblEncoderColumns[colIndex] = col
            myLblEncoders[col] = LabelEncoder()
            df[col] = myLblEncoders[col].fit_transform(df[col].astype(str))
            colIndex = colIndex + 1

        # Define the folder name
        folder_name = 'datasets\\' + str(self.data[0]) + '\\label_encoders'

        # Create the folder if it doesn't exist
        if not os.path.exists(folder_name):
            os.mkdir(folder_name)

        # Define the file path, including the folder
        merged_file_path = os.path.join(folder_name, 'merged_label_encoders.pkl')
        with open(merged_file_path, 'wb') as merged_file:
            pickle.dump(myLblEncoders, merged_file)

        self.save_updated_dataset(df)
        self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?", (self.data[0], 8))
        self.update_data_tasks()
        self.clear_grid_layout()
        self.analyze_dataset(df, True)

    def display_describe(self, df):
        child_window = tk.Toplevel(self.root)
        child_window.title('DataFrame Details')
        child_window.grab_set()

        screen_width = child_window.winfo_screenwidth()
        screen_height = child_window.winfo_screenheight()

        w = screen_width - 50
        h = 500

        x = (screen_width - w) // 2
        y = (screen_height - h) // 2

        child_window.geometry(f"{w}x{h}+{x}+{y}")

        describe_text = scrolledtext.ScrolledText(child_window, wrap=tk.WORD, width=w - 50, height=h - 50)
        describe_text.pack()

        describe_output = df.describe().to_string()
        describe_text.delete(1.0, tk.END)  # Clear any existing text
        describe_text.insert(tk.END, describe_output)

        child_window.wait_window(child_window)
        self.root.grab_set()  # Re-grab ChildForm

    # Function to display the DataFrame in a child window
    def display_dataframe(self, df):
        # Create a new child window
        child_window = tk.Toplevel(self.root)
        child_window.title('DataFrame Viewer')
        child_window.grab_set()

        screen_width = child_window.winfo_screenwidth()
        screen_height = child_window.winfo_screenheight()

        w = screen_width - 200
        h = screen_height - 150

        x = (screen_width - w) // 2
        y = (screen_height - h) // 2

        child_window.geometry(f"{w}x{h}+{x}+{y}")

        df = df.head(500)

        # Create a Pandas DataFrame as a Table
        tree = ttk.Treeview(child_window, columns=list(df.columns), show='headings', height=h - 100)

        # Define the columns
        for col in df.columns:
            tree.heading(col, text=col)
            tree.column(col, width=150)  # Adjust the width as needed

        # Insert data into the treeview
        for i in range(len(df)):
            values = [df[col][i] for col in df.columns]
            tree.insert('', 'end', values=values)

        # Create a vertical scrollbar
        vsb = ttk.Scrollbar(child_window, orient="vertical", command=tree.yview)
        vsb.pack(side="right", fill="y")

        # Create a horizontal scrollbar
        hsb = ttk.Scrollbar(child_window, orient="horizontal", command=tree.xview)
        hsb.pack(side="bottom", fill="x")

        # Configure the Treeview to use both vertical and horizontal scrollbars
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.pack()

        child_window.wait_window(child_window)
        self.root.grab_set()  # Re-grab ChildForm

    def drop_empty_columns(self, dataset):
        dataset = dataset.dropna(axis=1, how='all')  # Drop columns where all values are empty or null
        self.save_updated_dataset(dataset)
        self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?", (self.data[0], 1))
        self.update_data_tasks()
        self.clear_grid_layout()
        self.analyze_dataset(dataset, True)

    def drop_columns(self, dataset, columns_to_drop):
        dataset_copy = dataset.copy()
        dataset_copy.drop(columns=columns_to_drop, inplace=True)
        # dataset.drop(columns=columns_to_drop, inplace=True)
        self.save_updated_dataset(dataset_copy)
        self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?", (self.data[0], 2))
        self.update_data_tasks()
        self.clear_grid_layout()
        self.analyze_dataset(dataset_copy, True)

    def save_updated_dataset(self, dataset):
        # Define the folder name
        folder_name = 'datasets\\' + str(self.data[0]) + '\\cleaned'

        # Create the folder if it doesn't exist
        if not os.path.exists(folder_name):
            os.mkdir(folder_name)

        # Define the file path, including the folder
        file_path = os.path.join(folder_name, self.data[1])

        # Save the DataFrame to the CSV file in the new folder
        dataset.to_csv(file_path, index=False)

    def clear_grid_layout(self):
        # Get a list of all widgets in the grid
        widgets = self.frame.winfo_children()

        # Destroy each widget to clear the grid
        for widget in widgets:
            widget.destroy()

    def handle_missing_values(self, ds):
        if self.missing_data_form is None:
            missing_data_window = tk.Toplevel(self.root)
            child_form = MissingData(missing_data_window, self.data, self.close_missing_data_form)
            child_form.focus_child_form()
            missing_data_window.wait_window(missing_data_window)
            self.root.grab_set()  # Re-grab ChildForm

    def handle_negative_values(self, ds):
        if self.negative_data_form is None:
            negative_data_window = tk.Toplevel(self.root)
            child_form = NegativeData(negative_data_window, self.data, self.close_negative_data_form)
            child_form.focus_child_form()
            negative_data_window.wait_window(negative_data_window)
            self.root.grab_set()  # Re-grab ChildForm

    def close_negative_data_form(self):
        self.negative_data_form = None
        self.clear_grid_layout()
        self.load_dataset()

    def close_missing_data_form(self):
        self.missing_data_form = None
        self.clear_grid_layout()
        self.load_dataset()

    def handle_outliers(self, ds, columns):
        dataset = ds.copy()

        # for column in columns:
        #     Q1 = dataset[column].quantile(0.25)
        #     Q3 = dataset[column].quantile(0.75)
        #     IQR = Q3 - Q1
        #     lower_bound = Q1 - 1.5 * IQR
        #     upper_bound = Q3 + 1.5 * IQR
        #
        #     median_value = dataset[column].median()
        #     #dataset[column] = dataset[column].apply(lambda x: median_value if x < lower_bound else x)
        #     #dataset[column] = dataset[column].apply(lambda x: median_value if x > upper_bound else x)
        #     dataset.loc[dataset[column] < lower_bound, column] = median_value
        #     dataset.loc[dataset[column] > upper_bound, column] = median_value

        for column in columns:
            # Calculate Z-scores using the MAD for threshold determination
            median = dataset[column].median()
            mad = np.median(np.abs(dataset[column] - median))
            z_scores = np.abs((dataset[column] - median) / mad)

            # Automatically determine the threshold based on a quantile (e.g., 99th percentile)
            z_threshold = np.percentile(z_scores, 99)  # You can adjust the percentile as needed

            # Identify outliers
            outlier_indices = np.where(z_scores > z_threshold)

            median_value = dataset[column].median()
            dataset[column] = dataset[column].apply(lambda x: median_value if x in outlier_indices[0] else x)

        self.clear_grid_layout()
        self.analyze_dataset(dataset, True)

    def split_unique_columns(self, dataset):
        df = dataset.copy()
        # Find columns with all unique values
        unique_columns = [col for col in df.columns if df[col].nunique() == df.shape[0]]
        # Create a new DataFrame containing only the unique columns
        unique_df = df[unique_columns].copy()

        # Define the folder name
        folder_name = 'datasets\\' + str(self.data[0]) + '\\splitted'

        # Create the folder if it doesn't exist
        if not os.path.exists(folder_name):
            os.mkdir(folder_name)

        # Define the file path, including the folder
        file_path = os.path.join(folder_name, self.data[1])

        # Save the DataFrame to the CSV file in the new folder
        unique_df.to_csv(file_path, index=False)

        # Optionally, remove these columns from the original DataFrame
        df.drop(columns=unique_columns, inplace=True)

        self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?", (self.data[0], 2))
        self.update_data_tasks()

        self.save_updated_dataset(df)
        self.clear_grid_layout()
        self.analyze_dataset(df, True)

    def show_duplicates(self, dataset):
        duplicate_rows = dataset[dataset.duplicated(keep=False)]
        print(duplicate_rows)

    def load_modified_data_statistics(self, dataset):

        rows = dataset.shape[0]
        cols = dataset.shape[1]

        row_num = 1
        col_num = 3

        tk.Label(self.frame, text="", width=10).grid(row=0, column=0, sticky=tk.NSEW)
        tk.Label(self.frame, text="Data after cleaning", width=30, relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                          sticky=tk.NSEW)
        row_num += 1
        tk.Label(self.frame, text=str(rows), width=20, relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                              sticky=tk.NSEW)
        row_num += 1
        tk.Label(self.frame, text=str(cols), relief=tk.GROOVE).grid(row=row_num, column=col_num, sticky=tk.NSEW)
        row_num += 1

        # Find columns with all unique values
        unique_columns = [col for col in dataset.columns if dataset[col].nunique() == dataset.shape[0]]

        if len(unique_columns) > 0:
            tk.Label(self.frame, text=str(unique_columns), relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                  sticky=tk.NSEW)

            tk.Button(self.frame, text="Drop columns", relief=tk.GROOVE,
                      command=lambda ds=dataset: self.split_unique_columns(ds)).grid(
                row=row_num, column=col_num + 1, sticky=tk.NSEW)
        else:
            tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=col_num, sticky=tk.NSEW)
            if len(unique_columns) == 0 and self.data_tasks_dict[2] == 0:
                self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?, ?", (self.data[0], 2, 2))
                self.update_data_tasks()

        row_num += 1

        empty_columns = dataset.columns[dataset.isnull().all()].tolist()  # get names of columns with all values empty

        if len(empty_columns) > 0:
            tk.Label(self.frame, text=str(empty_columns), relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                 sticky=tk.NSEW)
            tk.Button(self.frame, text="Drop empty columns", relief=tk.GROOVE,
                      command=lambda ds=dataset: self.drop_empty_columns(ds)).grid(
                row=row_num, column=col_num + 1, sticky=tk.NSEW)
        else:
            tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                 sticky=tk.NSEW)

        row_num += 1

        # has_duplicates = dataset.duplicated().any()
        # tk.Label(self.frame, text=str(has_duplicates), relief=tk.GROOVE).grid(row=row_num, column=col_num,
        #                                                                       sticky=tk.NSEW)
        # if has_duplicates is not False:
        #     tk.Button(self.frame, text="Show duplicates", relief=tk.GROOVE,
        #               command=lambda ds=dataset: self.show_duplicates(dataset)).grid(
        #         row=row_num, column=col_num + 1, sticky=tk.NSEW)

        # row_num += 1

        has_missing_values = dataset.isnull().any().any()
        tk.Label(self.frame, text=str(has_missing_values), relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                  sticky=tk.NSEW)

        row_num += 1

        unique_counts = dataset.nunique()
        columns_with_same_values = unique_counts[unique_counts == 1].index.tolist()

        if len(columns_with_same_values) > 0:
            tk.Label(self.frame, text=str(columns_with_same_values), relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                            sticky=tk.NSEW)
            tk.Button(self.frame, text="Drop same value columns", relief=tk.GROOVE,
                      command=lambda ds=dataset, columns_to_drop=columns_with_same_values: self.drop_columns(ds,
                                                                                                             columns_to_drop)).grid(
                row=row_num, column=col_num + 1, sticky=tk.NSEW)
        else:
            tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                 sticky=tk.NSEW)

        row_num += 1

        # Select columns with numeric data types
        numeric_columns_data = dataset.select_dtypes(include=[int, float])

        # Identify columns with negative values
        columns_with_negatives = numeric_columns_data.columns[(numeric_columns_data < 0).any()].tolist()

        if len(columns_with_negatives) > 0:
            tk.Label(self.frame, text=str(columns_with_negatives), relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                          sticky=tk.NSEW)
            tk.Button(self.frame, text="Handle negative values", relief=tk.GROOVE,
                      command=lambda ds=dataset: self.handle_negative_values(ds)).grid(row=row_num, column=col_num + 1,
                                                                                       sticky=tk.NSEW)
        else:
            tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                 sticky=tk.NSEW)

        row_num += 1

        # # Create an empty list to store the columns with outliers
        # columns_with_outliers = []
        #
        # # Define your outlier detection criteria, for example, using the IQR method
        # for column in dataset.select_dtypes(include=['int64', 'float64']).columns:
        #     if not dataset[column].isnull().all():  # Exclude columns with all null values
        #         Q1 = dataset[column].quantile(0.25)
        #         Q3 = dataset[column].quantile(0.75)
        #         IQR = Q3 - Q1
        #         lower_bound = Q1 - 1.5 * IQR
        #         upper_bound = Q3 + 1.5 * IQR
        #
        #         # Check if the column has outliers based on your criteria
        #         if (dataset[column] < lower_bound).any() or (dataset[column] > upper_bound).any():
        #             columns_with_outliers.append(column)
        #
        # if len(columns_with_outliers) > 0:
        #     tk.Label(self.frame, text=str(columns_with_outliers), relief=tk.GROOVE).grid(row=row_num, column=col_num,
        #                                                                                  sticky=tk.NSEW)
        #     tk.Button(self.frame, text="Handle outliers", relief=tk.GROOVE,
        #               command=lambda ds=dataset, columns=columns_with_outliers: self.handle_outliers(ds,
        #                                                                                              columns_with_outliers)).grid(
        #         row=row_num, column=col_num + 1,
        #         sticky=tk.NSEW)
        # else:
        #     tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=col_num,
        #                                                          sticky=tk.NSEW)
        #
        # row_num += 1

        # Get numeric columns (int64 and float64)
        numeric_columns = dataset.select_dtypes(include=['int64', 'float64']).columns.tolist()
        tk.Label(self.frame, wraplength=450, text=str(numeric_columns), relief=tk.GROOVE).grid(row=row_num,
                                                                                               column=col_num,
                                                                                               sticky=tk.NSEW)
        row_num += 1

        # Find numeric columns with missing values
        columns_with_missing_values = dataset[numeric_columns].columns[dataset[numeric_columns].isnull().any()].tolist()
        tk.Label(self.frame, wraplength=450, text=str(columns_with_missing_values), relief=tk.GROOVE).grid(row=row_num,
                                                                                                           column=col_num,
                                                                                                           sticky=tk.NSEW)
        if len(columns_with_missing_values) > 0:
            tk.Button(self.frame, text="Handle missing values", relief=tk.GROOVE,
                      command=lambda ds=dataset: self.handle_missing_values(ds)).grid(row=row_num, column=col_num + 1,
                                                                                      sticky=tk.NSEW)
        row_num += 1

        # Get categorical columns (object)
        categorical_columns = dataset.select_dtypes(include=['object']).columns.tolist()
        tk.Label(self.frame, wraplength=450, text=str(categorical_columns), relief=tk.GROOVE).grid(row=row_num,
                                                                                                   column=col_num,
                                                                                                   sticky=tk.NSEW)
        if len(categorical_columns) > 0:
            tk.Button(self.frame, text="Label Encode Categorical Columns", relief=tk.GROOVE,
                      command=lambda df=dataset: self.encode_categorical_columns(dataset)).grid(row=row_num,
                                                                                                column=col_num + 1,
                                                                                                sticky=tk.NSEW)

        row_num += 1

        tk.Button(self.frame, text="Show cleaned data", relief=tk.GROOVE,
                  command=lambda df=dataset: self.display_dataframe(dataset)).grid(row=row_num, column=col_num)

        row_num += 1

        tk.Button(self.frame, text="Describe dataset", relief=tk.GROOVE,
                  command=lambda df=dataset: self.display_describe(dataset)).grid(row=row_num, column=col_num)

    def analyze_dataset(self, dataset, data_is_modified):

        orig_dataset = self.dataset

        rows = orig_dataset.shape[0]
        cols = orig_dataset.shape[1]

        row_num = 1
        col_num = 1

        tk.Label(self.frame, text="", width=10).grid(row=0, column=0, sticky=tk.NSEW)
        tk.Label(self.frame, text="Data before cleaning", width=30, relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                           sticky=tk.NSEW, columnspan=2)
        row_num += 1
        tk.Label(self.frame, text="Number of rows", relief=tk.GROOVE).grid(row=row_num, column=col_num, sticky=tk.NSEW)
        tk.Label(self.frame, text=str(rows), width=20, relief=tk.GROOVE).grid(row=row_num, column=2, sticky=tk.NSEW)
        row_num += 1
        tk.Label(self.frame, text="Number of columns", relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                              sticky=tk.NSEW)
        tk.Label(self.frame, text=str(cols), relief=tk.GROOVE).grid(row=row_num, column=2, sticky=tk.NSEW)
        row_num += 1

        # Find columns with all unique values
        unique_columns = [col for col in orig_dataset.columns if orig_dataset[col].nunique() == orig_dataset.shape[0]]
        tk.Label(self.frame, text="Columns with all unique values", relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                           sticky=tk.NSEW)
        if len(unique_columns) > 0:
            tk.Label(self.frame, text=str(unique_columns), relief=tk.GROOVE).grid(row=row_num, column=2, sticky=tk.NSEW)
        else:
            tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=2, sticky=tk.NSEW)
            if len(unique_columns) == 0 and self.data_tasks_dict[2] == 0:
                self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?, ?", (self.data[0], 2, 2))
                self.update_data_tasks()

        if len(unique_columns) > 0 and data_is_modified is not True:
            tk.Button(self.frame, text="Drop columns", relief=tk.GROOVE,
                      command=lambda ds=orig_dataset: self.split_unique_columns(ds)).grid(
                row=row_num, column=3, sticky=tk.NSEW)

        row_num += 1

        empty_columns = orig_dataset.columns[
            orig_dataset.isnull().all()].tolist()  # get names of columns with all values empty
        tk.Label(self.frame, text="Empty columns", relief=tk.GROOVE).grid(row=row_num, column=col_num, sticky=tk.NSEW)

        if len(empty_columns) > 0:
            tk.Label(self.frame, text=str(empty_columns), relief=tk.GROOVE).grid(row=row_num, column=2, sticky=tk.NSEW)
        else:
            tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=2, sticky=tk.NSEW)
            if len(empty_columns) == 0 and self.data_tasks_dict[1] == 0:
                self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?, ?", (self.data[0], 1, 2))
                self.update_data_tasks()

        if len(empty_columns) > 0 and data_is_modified is not True:
            tk.Button(self.frame, text="Drop empty columns", relief=tk.GROOVE,
                      command=lambda ds=orig_dataset: self.drop_empty_columns(ds)).grid(
                row=row_num, column=3, sticky=tk.NSEW)

        row_num += 1

        # has_duplicates = orig_dataset.duplicated().any()
        #
        # if has_duplicates is not True and self.data_tasks_dict[4] == 0:
        #     self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?, ?", (self.data[0], 4, 2))
        #     self.update_data_tasks()
        #
        # tk.Label(self.frame, text="Contains duplicate rows", relief=tk.GROOVE).grid(row=row_num, column=col_num,
        #                                                                             sticky=tk.NSEW)
        # tk.Label(self.frame, text=str(has_duplicates), relief=tk.GROOVE).grid(row=row_num, column=2, sticky=tk.NSEW)
        # row_num += 1

        has_missing_values = orig_dataset.isnull().any().any()
        tk.Label(self.frame, text="Contains missing values", relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                    sticky=tk.NSEW)
        tk.Label(self.frame, text=str(has_missing_values), relief=tk.GROOVE).grid(row=row_num, column=2, sticky=tk.NSEW)
        row_num += 1

        unique_counts = orig_dataset.nunique()
        columns_with_same_values = unique_counts[unique_counts == 1].index.tolist()
        tk.Label(self.frame, text="Columns with same values in all rows", relief=tk.GROOVE).grid(row=row_num,
                                                                                                 column=col_num,
                                                                                                 sticky=tk.NSEW)
        if len(columns_with_same_values) > 0:
            tk.Label(self.frame, text=str(columns_with_same_values), relief=tk.GROOVE).grid(row=row_num, column=2,
                                                                                            sticky=tk.NSEW)
        else:
            tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=2,
                                                                 sticky=tk.NSEW)

        if len(columns_with_same_values) > 0 and data_is_modified is not True:
            orig_dataset_copy = orig_dataset.copy()
            tk.Button(self.frame, text="Drop same value columns", relief=tk.GROOVE,
                      command=lambda ds=orig_dataset_copy, columns_to_drop=columns_with_same_values: self.drop_columns(
                          ds,
                          columns_to_drop)).grid(
                row=row_num, column=3, sticky=tk.NSEW)
        row_num += 1

        # Select columns with numeric data types
        numeric_columns_data = orig_dataset.select_dtypes(include=[int, float])

        # Identify columns with negative values
        columns_with_negatives = numeric_columns_data.columns[(numeric_columns_data < 0).any()].tolist()
        tk.Label(self.frame, text="Columns with negative values", relief=tk.GROOVE).grid(row=row_num,
                                                                                         column=col_num,
                                                                                         sticky=tk.NSEW)
        if len(columns_with_negatives) > 0:
            tk.Label(self.frame, text=str(columns_with_negatives), relief=tk.GROOVE).grid(row=row_num, column=2,
                                                                                          sticky=tk.NSEW)
        else:
            tk.Label(self.frame, text="", relief=tk.GROOVE).grid(row=row_num, column=2,
                                                                 sticky=tk.NSEW)
            if self.data_tasks_dict[9] == 0:
                self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?, ?", (self.data[0], 9, 2))
                self.update_data_tasks()

        row_num += 1

        # Get numeric columns (int64 and float64)
        numeric_columns = orig_dataset.select_dtypes(include=['int64', 'float64']).columns.tolist()

        tk.Label(self.frame, text="Numerical columns", relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                              sticky=tk.NSEW)
        tk.Label(self.frame, wraplength=450, text=str(numeric_columns), relief=tk.GROOVE).grid(row=row_num, column=2,
                                                                                               sticky=tk.NSEW)
        row_num += 1

        # Find numeric columns with missing values
        columns_with_missing_values = orig_dataset[numeric_columns].columns[
            orig_dataset[numeric_columns].isnull().any()].tolist()
        tk.Label(self.frame, text="Numerical columns with missing values", relief=tk.GROOVE).grid(row=row_num,
                                                                                                  column=col_num,
                                                                                                  sticky=tk.NSEW)
        tk.Label(self.frame, wraplength=450, text=str(columns_with_missing_values), relief=tk.GROOVE).grid(row=row_num,
                                                                                                           column=2,
                                                                                                           sticky=tk.NSEW)

        print(data_is_modified)
        print(len(columns_with_missing_values))

        if len(columns_with_missing_values) > 0 and data_is_modified is not True:
            orig_dataset_copy = orig_dataset.copy()
            tk.Button(self.frame, text="Handle missing values", relief=tk.GROOVE,
                      command=lambda ds=orig_dataset_copy: self.handle_missing_values(ds)).grid(row=row_num, column=3,
                                                                                      sticky=tk.NSEW)

        if len(columns_with_missing_values) == 0 and self.data_tasks_dict[3] == 0:
            self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?, ?", (self.data[0], 3, 2))
            self.update_data_tasks()

        row_num += 1

        # Get categorical columns (object)
        categorical_columns = orig_dataset.select_dtypes(include=['object']).columns.tolist()
        tk.Label(self.frame, text="Categorical columns", relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                sticky=tk.NSEW)
        tk.Label(self.frame, wraplength=450, text=str(categorical_columns), relief=tk.GROOVE).grid(row=row_num,
                                                                                                   column=2,
                                                                                                   sticky=tk.NSEW)

        if len(categorical_columns) == 0 and self.data_tasks_dict[8] == 0:
            self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?, ?", (self.data[0], 8, 2))
            self.update_data_tasks()

        row_num += 1

        tk.Button(self.frame, text="Show original data", relief=tk.GROOVE,
                  command=lambda df=orig_dataset: self.display_dataframe(orig_dataset)).grid(row=row_num, column=2)

        row_num += 1

        tk.Button(self.frame, text="Describe dataset", relief=tk.GROOVE,
                  command=lambda df=orig_dataset: self.display_describe(orig_dataset)).grid(row=row_num, column=2)

        if data_is_modified:
            self.load_modified_data_statistics(dataset)

      
