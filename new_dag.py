from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import time
import pandas as pd
import psycopg2
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'internship_scraper_email_pipeline',
    default_args=default_args,
    description='Scrape Glassdoor jobs, store in PostgreSQL, and send an email',
    schedule_interval='@daily',
)

# Task 1: Scrape job listings from Glassdoor
def scrape_glassdoor_jobs():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get("https://www.glassdoor.com/Job/united-states-data-analyst-jobs-SRCH_IL.0,13_IN1_KO14,26.htm")
    time.sleep(5)
    
    job_titles = driver.find_elements(By.CLASS_NAME, "JobCard_jobTitle___7I6y")
    job_locations = driver.find_elements(By.CLASS_NAME, "JobCard_location__rCz3x")
    job_companies = driver.find_elements(By.CLASS_NAME, 'EmployerProfile_compactEmployerName__LE242')
    
    job_listings = []
    num_jobs = len(job_titles)
    
    for i in range(num_jobs):
        title = job_titles[i].text if i < len(job_titles) else "N/A"
        location = job_locations[i].text if i < len(job_locations) else "N/A"
        company = job_companies[i].text if i < len(job_companies) else "N/A"
        job_listings.append({
            "Job Title": title,
            "Location": location,
            "Company": company
        })
    
    df = pd.DataFrame(job_listings)
    df.to_csv("airflow/glassdoor_job_listings.csv", index=False)
    driver.quit()

# Task 2: Store data in PostgreSQL
def store_in_postgresql():
    conn = psycopg2.connect(host="localhost", database="sql_analysis", user="postgres", password="Schavan#", port=5432)
    cur = conn.cursor()
    
    df = pd.read_csv("airflow/glassdoor_job_listings.csv")
    
    for _, row in df.iterrows():
        cur.execute("INSERT INTO internships (title, company, location) VALUES (%s, %s, %s)",
                    (row['Job Title'], row['Company'], row['Location']))
    
    conn.commit()
    cur.close()
    conn.close()

# Task 3: Query data and send email
def send_report_via_email():
    conn_str = "postgresql+psycopg2://postgres:Schavan#@localhost/sql_analysis"
    engine = create_engine(conn_str)
    conn = engine.connect()
    
    query = "SELECT title, company, location FROM internships WHERE title ILIKE '%%Data%%'"
    df = pd.read_sql(query, conn)
    
    report_path = '/path/to/save/filtered_internships_report.csv'
    df.to_csv(report_path, index=False)
    
    conn.close()
    engine.dispose()
    
    # Send email
    sender_email = "ceratopsezpz5@gmail.com"
    recipient_email = "soujanyachavan25@gmail.com"
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "Daily Internship Report"
    
    msg.attach(MIMEText("Please find attached the report.", 'plain'))
    
    attachment = open(report_path, 'rb')
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename=report.csv')
    msg.attach(part)
    
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, 'hxde kngk vabg noyz')
    server.sendmail(sender_email, recipient_email, msg.as_string())
    server.quit()

# Define the tasks in the DAG
scrape_task = PythonOperator(
    task_id='scrape_glassdoor_jobs',
    python_callable=scrape_glassdoor_jobs,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store_in_postgresql',
    python_callable=store_in_postgresql,
    dag=dag,
)

email_task = PythonOperator(
    task_id='send_report_via_email',
    python_callable=send_report_via_email,
    dag=dag,
)

# Set the task dependencies
scrape_task >> store_task >> email_task
