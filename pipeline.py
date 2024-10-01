import time
import pandas as pd
import psycopg2
import smtplib
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Function to initialize WebDriver and scrape job listings
def scrape_jobs():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get("https://www.glassdoor.com/Job/united-states-data-analyst-jobs-SRCH_IL.0,13_IN1_KO14,26.htm")
    time.sleep(5)

    job_titles = driver.find_elements(By.CLASS_NAME, "JobCard_jobTitle___7I6y")
    job_locations = driver.find_elements(By.CLASS_NAME, "JobCard_location__rCz3x")
    job_companies = driver.find_elements(By.CLASS_NAME, 'EmployerProfile_compactEmployerName__LE242')

    job_listings = []
    num_jobs = len(job_titles)

    for i in range(num_jobs):
        try:
            title = job_titles[i].text if i < len(job_titles) else "N/A"
            location = job_locations[i].text if i < len(job_locations) else "N/A"
            company = job_companies[i].text if i < len(job_companies) else "N/A"
            
            job_listings.append({
                "Job Title": title,
                "Location": location,
                "Company": company
            })
        except Exception as e:
            print(f"Error scraping job {i}: {e}")
            continue

    driver.quit()
    return pd.DataFrame(job_listings)

# Function to insert job listings into PostgreSQL database
def insert_into_db(df):
    conn = psycopg2.connect(
        host="localhost",
        database="sql_analysis",
        user="postgres",
        password="Schavan#",
        port=5432
    )
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO internships (title, company, location) VALUES (%s, %s, %s)
        """, (row['Job Title'], row['Company'], row['Location']))
    conn.commit()
    cur.close()
    conn.close()

# Function to generate report from the database
def generate_report():
    conn_str = "postgresql+psycopg2://postgres:Schavan#@localhost/sql_analysis"
    engine = create_engine(conn_str)
    
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT title, company, location
                FROM internships
                WHERE title ILIKE '%%Data%%'
            """)
            result_proxy = conn.execute(query)
            results = result_proxy.fetchall()
            columns = result_proxy.keys()
            df = pd.DataFrame(results, columns=columns)

            if not df.empty:
                report_path = 'filtered_internships_report.csv'
                df.to_csv(report_path, index=False)
                print("Report generated successfully!")
                return report_path
            else:
                print("No data found for the query.")
                return None
    except Exception as e:
        print(f"Error executing the query: {e}")
    finally:
        engine.dispose()

# Function to send email with the report
def send_email_with_report(report_path):
    if report_path is None:
        print("No report to send.")
        return

    sender_email = "ceratopsezpz5@gmail.com"
    recipient_email = "soujanyachavan25@gmail.com"
    subject = "Daily Internship Report"
    body = "Please find attached the latest report on summer internships."
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        attachment = open(report_path, 'rb')
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={report_path}')
        msg.attach(part)
    except Exception as e:
        print(f"Error attaching the file: {e}")
        return

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, 'hxde kngk vabg noyz')  # Use App Password here
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")

# Main function to run the complete pipeline
def main():
    # Step 1: Scrape Jobs
    df = scrape_jobs()
    
    # Step 2: Insert into Database
    insert_into_db(df)
    
    # Step 3: Generate Report
    report_path = generate_report()
    
    # Step 4: Send Email with Report
    send_email_with_report(report_path)

# Execute the pipeline
if __name__ == "__main__":
    main()
