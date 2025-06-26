import pandas as pd
import tkinter as tk
from tkinter.filedialog import askopenfile, asksaveasfile, asksaveasfilename
from tkinter import *
import numpy as np
import logging
import threading
import seaborn as sns
import pickle
from sklearn.preprocessing import LabelEncoder
from tkinter.scrolledtext import ScrolledText
import sys


class TextHandler(logging.Handler):
   """This class allows you to log to a Tkinter Text or ScrolledText widget"""

   def __init__(self, text, tkObj):
      # run the regular Handler __init__
      logging.Handler.__init__(self)
      # Store a reference to the Text it will log to
      self.text = text
      self.tkObj = tkObj

   def emit(self, record):
      msg = self.format(record)

      def append():
         self.text.configure(state='normal')
         self.text.insert(self.tkObj.END, msg + '\n')
         self.text.configure(state='disabled')
         # Autoscroll to the bottom
         self.text.yview(self.tkObj.END)

      # This is necessary because we can't modify the Text from other threads
      self.text.after(0, append)

class App(tk.Tk):
   def __init__(self):
      super().__init__()

      self.wm_title("Data Processing")

      self.geometry("800x800")


      self.scrolled_text_widget = ScrolledText(self, width=90, height=15, state='disabled')
      self.scrolled_text_widget.configure(font='TkFixedFont')
      self.scrolled_text_widget.pack()
      self.redirect_output()



      # Create textLogger
      #text_handler = TextHandler(st, tk)

      # Add the handler to logger
      #logger = logging.getLogger()
      #logger.addHandler(text_handler)

      #self.logger = logger

      varYloc = 300

      self.clean_dataset = tk.IntVar()
      self.handle_outliers = tk.IntVar()
      self.encode_dataset = tk.IntVar()
      c1 = tk.Checkbutton(self, text='Clean dataset', variable=self.clean_dataset, onvalue=1, offvalue=0)
      c1.place(x=50, y=varYloc)
      varYloc = varYloc + 30
      c2 = tk.Checkbutton(self, text='Handle outliers', variable=self.handle_outliers, onvalue=1, offvalue=0)
      c2.place(x=50, y=varYloc)
      varYloc = varYloc + 30
      c3 = tk.Checkbutton(self, text='Encode dataset', variable=self.encode_dataset, onvalue=1, offvalue=0)
      c3.place(x=50, y=varYloc)
      #varYloc = varYloc + 30
      #c4 = tk.Checkbutton(self, text='Encode dataset', variable=var4, onvalue=1, offvalue=0)
      #c4.place(x=50, y=varYloc)
      varYloc = varYloc + 50
      B = Button(self, text="Upload File", command=self.showFileBrowse)
      B.place(x=50, y=varYloc)

   def redirect_output(self):
      class StdoutRedirector:
         def __init__(self, text_widget):
            self.text_widget = text_widget

         def write(self, text):
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, text)
            self.text_widget.insert(tk.END, "\n")
            self.text_widget.see(tk.END)  # Auto-scroll to the end
            self.text_widget.configure(state='disabled')

         def flush(self):
            pass  # Required for sys.stdout to work correctly

      sys.stdout = StdoutRedirector(self.scrolled_text_widget)

   def showFileBrowse(self):
      filename = askopenfile()
      #print(filename)
      self.cleanFile(filename)


   def cleanFile(self, fileName):

      # Get the full file path
      full_file_path = fileName.name

      # Extract the filename without extension
      file_name_without_extension = full_file_path.split('/')[-1].split('.')[0]

      data = pd.read_csv(fileName)
      print(data.shape)

      missing_values = data.isnull().sum() #get missing or empty values
      print("Missing Values:\n", missing_values)

      empty_columns = data.columns[data.isnull().all()] #get names of columns with all values empty
      if(empty_columns.size > 0):
         comma_separated_column_names = ', '.join(empty_columns)
         print('There are ' + str(empty_columns.size) + ' columns with all values empty. Columns: ' + comma_separated_column_names)
         data = data.dropna(axis=1, how='all') #Drop columns where all values are empty or null
         print(data.shape)

      missing_values = data.isnull().sum()
      print("Missing Values:\n", missing_values)

      # Find columns with missing values
      columns_with_missing_values = data.columns[data.isnull().any()].tolist()

      print("Columns with Missing Values:\n", columns_with_missing_values)

      # Replace missing values with the median of each respective column
      for column in columns_with_missing_values:
         median_value = data[column].median()
         data[column].fillna(median_value, inplace=True)

      has_missing_values = data.isnull().any().any() #Check if there are any missing values left in the dataset

      if has_missing_values:
         missing_values = data.isnull().sum()
         print("Missing Values:\n", missing_values)
      else:
         print("No missing values in dataset\n")



      # Check for duplicate records in the entire DataFrame
      has_duplicates = data.duplicated().any()
      if has_duplicates:
         print("There are some duplicate records. Removing duplicate records")
         # Removing Duplicates
         data = data.drop_duplicates()

      # Identify columns with all distinct values
      columns_to_drop = []
      for column in data.columns:
         if data[column].nunique() == len(data):
            columns_to_drop.append(column)

      if len(columns_to_drop) > 0:
         comma_separated_column_names = ', '.join(columns_to_drop)
         print('There are ' + str(len(columns_to_drop)) + ' columns with all values unique. Columns: ' + comma_separated_column_names)
         # Drop the identified columns
         data.drop(columns=columns_to_drop, inplace=True)
         print(data.shape)


      # Get all categorical columns
      #categorical_columns = data.select_dtypes(include=['object', 'category']).columns.tolist()

      # Print the categorical columns
      #print(categorical_columns)


      data_bkp = data.copy()

      #lblencoder = LabelEncoder()
      #for col in data.select_dtypes(include=['O']).columns:
         #data[col] = lblencoder.fit_transform(data[col].astype(str))

      # Define a threshold for the number of unique values to consider a column categorical
      categorical_threshold = data.shape[0] / 2

      # Initialize lists to store potential target features
      potential_classification_targets = []
      potential_regression_targets = []

      # Iterate through DataFrame columns
      for column in data.columns:
         # Get the data type of the column
         data_type = data[column].dtype

         # Calculate the number of unique values in the column
         num_unique = data[column].nunique()

         # Criteria for potential classification target:
         # - Categorical data type (object or category)
         # - Fewer unique values than the threshold
         if (data_type == 'object' or data_type == 'category') and (2 < num_unique <= categorical_threshold):
            potential_classification_targets.append(column)

         # Criteria for potential regression target:
         # - Numeric data type (int64 or float64)
         if np.issubdtype(data_type, np.number):
            potential_regression_targets.append(column)

      # Print the lists of potential classification and regression targets
      print("Potential Classification Targets:", potential_classification_targets)
      print("Potential Regression Targets:", potential_regression_targets)



      initial_file_name = file_name_without_extension + "_Cleaned.csv"

      # Ask the user to choose a file location for saving
      file_path = asksaveasfilename(defaultextension=".csv",
                                    filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
                                    initialfile=initial_file_name)

      # Check if the user canceled the operation
      if file_path:
         # Save the DataFrame to the selected CSV file
         data.to_csv(file_path, index=False)
         print(f"Data saved to {file_path}")
      else:
         print("User canceled the save operation")



app = App()
app.mainloop()