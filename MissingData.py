import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import pandas as pd
from DBHelper import DBHelper
import os
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
import threading
import time

class MissingData(tk.Frame):
    def __init__(self, root, data, close_callback):
        super(MissingData, self).__init__()
        self.root = root
        self.data = data
        self.dataset = None
        self.data_tasks = None
        self.data_tasks_dict = None
        root.title("Handle Missing Data")
        root.grab_set()

        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()

        w = screen_width - 200
        h = screen_height - 150

        x = (screen_width - w) // 2
        y = (screen_height - h) // 2

        root.geometry(f"{w}x{h}+{x}+{y}")

        self.frame = ttk.Frame(root)
        self.frame.pack(fill=tk.BOTH, expand=True)
        # self.frame.grid(row=0, column=0, sticky="w")

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
                self.dataset = pd.read_csv(file_path)

        if self.dataset is not None:
            self.load_missing_data_columns()

    def select_option(self, selected_value, item_row, col_name):
        # print("Selected option:", selected_value)
        # print("Row:", item_row)
        # print("Column:", col_name)
        tk.Button(self.frame, text='Process', relief=tk.GROOVE,
                  command=lambda action_id=selected_value, column_name=col_name: self.process_action(action_id,
                                                                                                     column_name)).grid(
            row=item_row, column=11, sticky=tk.NSEW)

    def process_action(self, action_id, column_name):
        #print(action_id)
        #print(column_name)
        dataset = self.dataset.copy()
        if action_id == 0:
            #print("Use Mean")
            dataset[column_name].fillna(dataset[column_name].mean(), inplace=True)
        elif action_id == 1:
            #print("Use Median")
            dataset[column_name].fillna(dataset[column_name].median(), inplace=True)
        elif action_id == 2:
            #print("Use Zero(0)")
            dataset[column_name].fillna(0, inplace=True)
        elif action_id == 3:
            dataset = self.predict_missing_value(column_name)
            #print("Predict Missing Values")

        self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?", (self.data[0], 3))
        self.update_data_tasks()

        self.save_updated_dataset(dataset)
        self.clear_grid_layout()
        self.load_dataset()

    def predict_missing_value(self, column_name):

        dialog = tk.Toplevel(self.root)

        # Remove close and minimize buttons from the window
        dialog.overrideredirect(True)

        # Make the loading window a transient dialog to block the parent form
        dialog.transient(self.root)

        dialog.lift()

        dialog.focus()
        dialog.grab_set()

        # Calculate the center position for the dialog
        screen_width = dialog.winfo_screenwidth()
        screen_height = dialog.winfo_screenheight()
        x = (screen_width - 400) // 2
        y = (screen_height - 150) // 2

        dialog.geometry(f"600x150+{x}+{y}")

        # Add a border
        dialog['borderwidth'] = 2

        # Add a background color
        dialog['background'] = 'light gray'

        # Create a label to display a message
        label = tk.Label(dialog, text="Predicting data. Please wait...")
        label.pack(padx=20, pady=20)

        # Create a determinate progress bar
        progress = ttk.Progressbar(dialog, mode='determinate', maximum=100)
        progress.pack(padx=20, pady=20)

        df = self.dataset.copy()

        def simulate_work():
            progress['value'] = 10
            dialog.update_idletasks()  # Update the progress bar
            dialog.update()

            # Separate data into complete and incomplete records
            complete_data = df.dropna()
            incomplete_data = df[df[column_name].isnull()]

            # Filter for numerical features only
            numerical_features = df.select_dtypes(include=[int, float])
            numerical_features = numerical_features.drop(column_name, axis=1)

            # Handle missing values in numerical columns
            imputer = SimpleImputer(strategy='mean')
            X_train = imputer.fit_transform(complete_data[numerical_features.columns])
            y_train = complete_data[column_name]

            progress['value'] = 20
            dialog.update_idletasks()  # Update the progress bar
            dialog.update()

            # Select the first 5000 rows for training
            num_rows_to_select = 5000
            X_train_subset = X_train[:num_rows_to_select]
            y_train_subset = y_train[:num_rows_to_select]

            progress['value'] = 30
            dialog.update_idletasks()  # Update the progress bar
            dialog.update()

            # Train a RandomForestRegressor model on the selected subset of data
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X_train_subset, y_train_subset)

            progress['value'] = 60
            dialog.update_idletasks()  # Update the progress bar
            dialog.update()

            # Handle missing values in numerical columns for incomplete data
            X_test = imputer.transform(incomplete_data[numerical_features.columns])
            predicted_values = model.predict(X_test)

            progress['value'] = 80
            dialog.update_idletasks()  # Update the progress bar
            dialog.update()

            # Fill in missing values in the original DataFrame
            df.loc[df[column_name].isnull(), column_name] = predicted_values

            progress['value'] = 100
            dialog.update_idletasks()  # Update the progress bar
            dialog.update()
            time.sleep(0.1)

            dialog.destroy()

        # Start the work in a separate thread
        work_thread = threading.Thread(target=simulate_work)
        work_thread.start()

        dialog.wait_window(dialog)
        self.root.grab_set()  # Re-grab ChildForm

        return df

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

    def load_missing_data_columns(self):
        row_num = 1
        col_num = 1

        dataset = self.dataset.copy()

        tk.Label(self.frame, text="", width=10).grid(row=0, column=0, sticky=tk.NSEW)
        tk.Label(self.frame, text="Column Name", width=30, relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                                  sticky=tk.NSEW)
        tk.Label(self.frame, text="Empty values count", width=20, relief=tk.GROOVE).grid(row=row_num,
                                                                                         column=col_num + 1,
                                                                                         sticky=tk.NSEW)
        tk.Label(self.frame, text="Min Value", width=20, relief=tk.GROOVE).grid(row=row_num,
                                                                                column=col_num + 2,
                                                                                sticky=tk.NSEW)
        tk.Label(self.frame, text="Max Value", width=20, relief=tk.GROOVE).grid(row=row_num,
                                                                                column=col_num + 3,
                                                                                sticky=tk.NSEW)

        tk.Label(self.frame, text="Average / Mean", width=20, relief=tk.GROOVE).grid(row=row_num,
                                                                                     column=col_num + 4,
                                                                                     sticky=tk.NSEW)

        tk.Label(self.frame, text="Median", width=20, relief=tk.GROOVE).grid(row=row_num,
                                                                             column=col_num + 5,
                                                                             sticky=tk.NSEW)

        tk.Label(self.frame, text="Action", width=20, relief=tk.GROOVE).grid(row=row_num,
                                                                             column=col_num + 6,
                                                                             sticky=tk.NSEW, columnspan=4)

        numeric_columns = dataset.select_dtypes(include=['int64', 'float64']).columns.tolist()
        columns_with_missing_values = dataset[numeric_columns].columns[dataset[numeric_columns].isnull().any()].tolist()

        if len(columns_with_missing_values) == 0:
            self.db.execute_query("EXEC Process.UpdateDataFileTask ?, ?", (self.data[0], 3))
            self.update_data_tasks()
            self.close()

        row_num += 1

        # Create a tkinter variable to hold the selected option

        # Sample data for radio buttons
        options = ["Use Mean", "Use Median", "Use Zero(0)", "Predict Missing Values"]

        radio_buttons = []

        for col in columns_with_missing_values:

            selected_option = tk.StringVar()

            # Create radio buttons using a loop and place them in the same row
            for i, option in enumerate(options):
                radio_button = ttk.Radiobutton(self.frame, text=option, variable=selected_option, value=option,
                                               command=lambda selected_value=i, item_row=row_num,
                                                              col_name=col: self.select_option(
                                                   selected_value, item_row, col_name))
                radio_button.grid(row=row_num, column=i + col_num + 6, sticky="w")
                radio_buttons.append(radio_button)

            selected_option.set(options[0])

            tk.Label(self.frame, text=col, relief=tk.GROOVE).grid(row=row_num, column=col_num,
                                                                  sticky=tk.NSEW)

            tk.Label(self.frame, text=dataset.isnull().sum()[col], relief=tk.GROOVE).grid(row=row_num,
                                                                                          column=col_num + 1,
                                                                                          sticky=tk.NSEW)

            tk.Label(self.frame, text=dataset[col].min(), relief=tk.GROOVE).grid(row=row_num,
                                                                                 column=col_num + 2,
                                                                                 sticky=tk.NSEW)

            tk.Label(self.frame, text=dataset[col].max(), relief=tk.GROOVE).grid(row=row_num,
                                                                                 column=col_num + 3,
                                                                                 sticky=tk.NSEW)

            tk.Label(self.frame, text=dataset[col].mean(), relief=tk.GROOVE).grid(row=row_num,
                                                                                  column=col_num + 4,
                                                                                  sticky=tk.NSEW)

            tk.Label(self.frame, text=dataset[col].median(), relief=tk.GROOVE).grid(row=row_num,
                                                                                    column=col_num + 5,
                                                                                    sticky=tk.NSEW)

            row_num += 1

    def focus_child_form(self):
        self.root.focus_set()

    def close(self):
        self.root.grab_release()
        self.root.destroy()
        self.close_callback()
