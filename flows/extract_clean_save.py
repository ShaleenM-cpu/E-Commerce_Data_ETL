from prefect import flow, task
import pandas as pd
import sqlite3
import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SENDER = os.getenv("EMAIL_USER")
PASSWORD = os.getenv("EMAIL_PASSWORD")
RECIPIENT = os.getenv("EMAIL_TO")

# File paths
RAW_PATH = r"D:\EU School\Academics\3rd Semester\Business Intelligence\Sharleen\E-Commerce ETL\Data\Raw\ecommerce_product_dataset.csv"
PROCESSED_CSV_PATH = r"D:\EU School\Academics\3rd Semester\Business Intelligence\Sharleen\E-Commerce ETL\Data\Processed\cleaned_data.csv"
PROCESSED_DB_PATH = r"D:\EU School\Academics\3rd Semester\Business Intelligence\Sharleen\E-Commerce ETL\Data\Processed\cleaned_data.db"
TABLE_NAME = "ecommerce_products"


def send_email_alert(subject: str, body: str):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SENDER
    msg["To"] = RECIPIENT

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER, PASSWORD)
            server.sendmail(SENDER, RECIPIENT, msg.as_string())
        print(f"📧 Email sent: {subject}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")


@task
def extract_data():
    print("📥 Reading raw data...")
    df = pd.read_csv(RAW_PATH)
    print(f"✅ Extracted {len(df)} rows")
    return df


@task
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("🧹 Cleaning data...")
    df = df.drop_duplicates()
    df['ProductName'] = df['ProductName'].astype(str).str.strip()
    df['Category'] = df['Category'].astype(str).str.strip()
    df['City'] = df['City'].astype(str).str.strip()

    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Discount'] = pd.to_numeric(df['Discount'], errors='coerce')
    df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce').fillna(0)
    df['NumReviews'] = pd.to_numeric(df['NumReviews'], errors='coerce').fillna(0).astype(int)
    df['StockQuantity'] = pd.to_numeric(df['StockQuantity'], errors='coerce').fillna(0).astype(int)
    df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce').fillna(0)

    df['DateAdded'] = pd.to_datetime(df['DateAdded'], errors='coerce')
    df['City'] = df['City'].fillna('Unknown')
    df['Category'] = df['Category'].fillna('Uncategorized')

    print("✅ Cleaning complete")
    return df


@task
def save_data(df: pd.DataFrame):
    print("💾 Saving cleaned data...")

    df.to_csv(PROCESSED_CSV_PATH, index=False)
    print(f"📁 Saved CSV to: {PROCESSED_CSV_PATH}")

    conn = sqlite3.connect(PROCESSED_DB_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
    conn.close()
    print(f"🗄️  Saved to SQLite DB: {PROCESSED_DB_PATH}")


@flow(name="manual-etl-run")
def etl_flow():
    print("🚀 Starting ETL flow...")
    try:
        raw_data = extract_data()
        cleaned_data = clean_data(raw_data)
        save_data(cleaned_data)

        send_email_alert(
            subject="✅ ETL Pipeline Completed",
            body="Your ETL flow completed successfully with no errors."
        )
        print("✅ ETL flow completed successfully.")

    except Exception as e:
        send_email_alert(
            subject="🚨 ETL Failure Alert!",
            body=f"Your ETL pipeline failed with the following error:\n\n{str(e)}"
        )


if __name__ == "__main__":
    etl_flow()
