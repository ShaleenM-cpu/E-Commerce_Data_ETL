E-Commerce Product Intelligence Project Summary
This project delivers a complete end-to-end data pipeline for analyzing e-commerce product performance. The goal is to extract, clean, and analyze product data, apply machine learning for classification, and automate ETL operations with email notifications. The dashboard and alerts provide actionable insights to improve business decisions.

Project Overview
We started by manually downloading a product dataset from Kaggle. The data contains 1,000 products with attributes such as price, category, sales, rating, reviews, and city. A local ETL flow was implemented using Python and Prefect. The cleaned data was stored in both CSV and SQLite formats, and model training was conducted using scikit-learn.

A Power BI dashboard was developed to visualize category trends, regional performance, and customer engagement. The ETL process includes email notifications that are automatically sent upon successful or failed executions.

Although a 2-minute scheduled deployment using Prefect was attempted, it failed consistently due to .env access issues during automated runs. Therefore, we proceeded with a manual ETL execution, which successfully performs all tasks and sends real-time email alerts using Gmail SMTP.

Tools and Technologies
Python: pandas, NumPy, SQLite3, scikit-learn

ETL Orchestration: Prefect 2.x

Automation: Gmail SMTP (via smtplib and python-dotenv)

Modeling: K-Nearest Neighbors (KNN)

Dashboard: Power BI Desktop

Email Notification: Success/failure alerts sent via .env credentials

ETL Flow Summary
Extraction:
The raw dataset was imported from a Kaggle CSV file. Data was loaded using pandas.

Transformation:
Data was cleaned by removing duplicates, formatting string fields, and converting numeric columns. Missing values were filled appropriately. The processed data was saved as both .csv and .db.

Machine Learning:
A KNN classifier was trained using raw features (price, reviews, stock, etc.). It achieved:

Accuracy: 0.695

F1 Score: 0.282
This model outperformed all other classifiers and was saved as model.pkl.

Automation and Alerting:
While automation via 2-minute intervals was attempted using Prefect deployments, only manual runs sent email alerts correctly. The send_email_alert() function uses Gmail SMTP, triggered inside the flow for success or failure. It loads credentials from a .env file.

Dashboard Summary
The Power BI dashboard visualizes key performance indicators:

Total Products, Average Rating, Total Reviews, Total Sales

Product distribution by category

Sales distribution across cities

Top reviewed products

Filterable insights by category and city

Sample insight:
“Pittsburgh recorded the highest product sales, particularly in the Electronics category.”

How the Project Runs
ETL is run manually using:

bash
Copy
Edit
python flows/extract_clean_save.py
Email is sent to the analyst inbox to confirm success or failure.

Data output is saved locally and consumed by Power BI for real-time visuals.

Conclusion
Despite scheduler limitations in Prefect, the manually run ETL process works reliably. It ensures clean, structured output and timely notifications. Combined with Power BI insights and KNN modeling, this project provides a practical and scalable framework for product intelligence in e-commerce environments.