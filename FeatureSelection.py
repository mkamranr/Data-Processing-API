import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from DBHelper import DBHelper
import os
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import RFE
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import SVR
from sklearn.metrics import mean_squared_error, r2_score

class FeatureSelection(tk.Frame):
    def __init__(self, root, data, close_callback):
        super(FeatureSelection, self).__init__()
        self.root = root
        self.data = data
        self.dataset = None
        self.data_tasks = None
        self.data_tasks_dict = None
        self.feature_combobox = None
        self.label = None
        self.feature_weights = None
        self.treeview = None
        self.heatmap_frame = None
        root.title("Features Selection")
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

        has_completed_tasks = any(row[4] == 1 or row[4] == 2 for row in self.data_tasks)

        if has_completed_tasks:
            folder_name = 'datasets\\' + str(self.data[0]) + '\\cleaned'
            file_path = os.path.join(folder_name, self.data[1])
            if os.path.exists(file_path):
                self.dataset = pd.read_csv(file_path)

        dataset = self.dataset.copy()

        if self.dataset is not None:
            self.load_features(dataset)

    def on_feature_selected(self, event):
        selected_feature = self.feature_combobox.get()
        self.label.config(text=f"Selected Feature: {selected_feature}")

        predict_button = ttk.Button(self.frame, text="Predict and Display Accuracy",
                                    command=lambda sel_feature=selected_feature: self.predict_and_display_accuracy(sel_feature))
        predict_button.grid(row=1, column=0, columnspan=2)

        self.calculate_feature_weights(selected_feature)
        correlation_matrix = self.calculate_correlation_matrix(selected_feature)
        self.display_correlation_heatmap(correlation_matrix)

    def predict_and_display_accuracy(self, sel_feature):

        dataset = self.dataset.copy()



        X = dataset.drop(columns=[sel_feature])
        y = dataset[sel_feature]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        algorithm_predictions = pd.DataFrame(y_test.values, columns=['Actual'])

        # Initialize and train different regression models
        regressors = {
            'Linear Regression': LinearRegression(),
            'Decision Tree Regressor': DecisionTreeRegressor()
        }



        results = {}

        for name, model in regressors.items():
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            algorithm_predictions[name] = y_pred
            mse = mean_squared_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            results[name] = {
                'Mean Squared Error': mse,
                'R-squared': r2
            }

        # Compare model performances
        for name, metrics in results.items():
            print(f'{name}:')
            print(f'Mean Squared Error: {metrics["Mean Squared Error"]:.2f}')
            print(f'R-squared: {metrics["R-squared"]:.2f}\n')

        print(algorithm_predictions)

        # model = LinearRegression()
        # model.fit(X_train, y_train)
        # y_pred = model.predict(X_test)
        #
        # accuracy = model.score(X_test, y_test)  # R-squared value
        #
        # print(f"Model Accuracy (R-squared): {accuracy}")
        # print(f"Mean Squared Error: {mean_squared_error(y_test, y_pred)}")
        #
        # # Get the top 10 predicted values and corresponding actual values
        # top_predicted_values = list(zip(y_pred[:20], y_test[:20]))
        # print("Top 10 Predicted vs Actual Values:")
        # for predicted, actual in top_predicted_values:
        #     print(f"Predicted: {predicted}, Actual: {actual}")

    def calculate_feature_weights(self, selected_feature):
        dataset = self.dataset.copy()
        X = dataset.drop(columns=[selected_feature])
        y = dataset[selected_feature]

        # Fit a linear regression model
        model = LinearRegression()
        model.fit(X, y)

        # Get the feature coefficients (weights)
        self.feature_weights = dict(zip(X.columns, model.coef_))
        self.display_feature_weights()

    def display_feature_weights(self):
        if self.treeview:
            self.treeview.destroy()

        self.treeview = ttk.Treeview(self.frame, columns=('Feature', 'Weight'))
        self.treeview.heading('#1', text='Feature')
        self.treeview.heading('#2', text='Weight')

        # Sort the features by weight in descending order
        sorted_features = sorted(self.feature_weights.items(), key=lambda x: x[1], reverse=True)

        for feature, weight in sorted_features:
            self.treeview.insert('', 'end', values=(feature, weight))

        self.treeview.grid(row=2, column=0, columnspan=2)

    def load_features(self, dataset):

        self.label = tk.Label(self.frame, text="Selected Feature:")
        self.label.grid(row=0, column=0)

        # Extract feature names from the DataFrame
        feature_names = list(dataset.columns)

        # Create a Combobox (Dropdown) to select features
        self.feature_combobox = ttk.Combobox(self.frame, values=feature_names)
        self.feature_combobox.grid(row=0, column=1)
        self.feature_combobox.bind("<<ComboboxSelected>>", self.on_feature_selected)

    def calculate_correlation_matrix(self, selected_feature):
        correlation_matrix = self.dataset.corr()
        return correlation_matrix

    def display_correlation_heatmap(self, correlation_matrix):

        print(correlation_matrix)

        if self.heatmap_frame:
            self.heatmap_frame.destroy()

        fig, ax = plt.subplots(figsize=(8, 8))

        # Create a mask for the upper triangle
        #mask = np.tri(correlation_matrix.shape[0], k=-1).T
        #mask = np.triu(np.ones(correlation_matrix.shape), k=1)
        mask = np.triu(np.ones(correlation_matrix.shape), k=0)

        # sns.heatmap(correlation_matrix[(correlation_matrix >= 0.5) | (correlation_matrix <= -0.4)],
        #     cmap='viridis', vmax=1.0, vmin=-1.0, linewidths=0.1,
        #     annot=True, annot_kws={"size": 9}, square=True, ax=ax)
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', mask=mask, ax=ax)
        ax.set_title("Correlation Heatmap")

        self.heatmap_frame = tk.Frame(self.frame)
        self.heatmap_frame.grid(row=2, column=5)
        canvas = FigureCanvasTkAgg(fig, master=self.heatmap_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(fill=tk.BOTH, expand=True)

    def focus_child_form(self):
        self.root.focus_set()

    def close(self):
        self.root.grab_release()
        self.root.destroy()
        self.close_callback()
