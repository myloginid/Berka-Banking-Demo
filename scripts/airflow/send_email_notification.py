#!/usr/bin/env python3
"""
Send email notification for pipeline completion.

This is a standalone script to be called by Airflow PythonOperator.

Usage in Airflow DAG:
    from scripts.airflow.send_email_notification import send_success_email
    
    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_success_email,
    )
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional


# Email configuration - Modify these values
SMTP_HOST = "mail38.onamae.ne.jp"
SMTP_PORT = 587
SMTP_USER = "info@zzeng.net"  # Replace with your email
SMTP_PASSWORD = "<MyPassword>"        # Replace with your password
FROM_EMAIL = "info@zzeng.net"
TO_EMAILS = ["zzeng@cloudera.com"]  # List of recipients


def send_email(
    subject: str,
    html_content: str,
    to_emails: Optional[list] = None,
    smtp_host: str = SMTP_HOST,
    smtp_port: int = SMTP_PORT,
    smtp_user: str = SMTP_USER,
    smtp_password: str = SMTP_PASSWORD,
    from_email: str = FROM_EMAIL,
):
    """
    Send an HTML email via SMTP.
    
    Args:
        subject: Email subject
        html_content: HTML body content
        to_emails: List of recipient email addresses
        smtp_host: SMTP server hostname
        smtp_port: SMTP server port
        smtp_user: SMTP username
        smtp_password: SMTP password
        from_email: Sender email address
    """
    print("[EMAIL] Starting email sending process...")
    
    if to_emails is None:
        to_emails = TO_EMAILS
    
    print(f"[EMAIL] Configuration: SMTP={smtp_host}:{smtp_port}, From={from_email}, To={', '.join(to_emails)}")
    print(f"[EMAIL] Subject: {subject}")
    
    # Create message
    print("[EMAIL] Creating MIME message...")
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(to_emails)
    
    # Attach HTML content
    html_part = MIMEText(html_content, 'html')
    msg.attach(html_part)
    print(f"[EMAIL] HTML content attached (length: {len(html_content)} chars)")
    
    # Send email
    try:
        print(f"[EMAIL] Connecting to SMTP server {smtp_host}:{smtp_port}...")
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)  # Add timeout
        server.set_debuglevel(1)  # Enable verbose output for debugging
        
        print("[EMAIL] Starting TLS encryption...")
        server.starttls()
        
        print(f"[EMAIL] Authenticating as {smtp_user}...")
        server.login(smtp_user, smtp_password)
        
        print(f"[EMAIL] Sending email to {len(to_emails)} recipient(s)...")
        server.sendmail(from_email, to_emails, msg.as_string())
        server.quit()
        
        print(f"✅ [EMAIL] Email sent successfully to {', '.join(to_emails)}")
        return True
    
    except smtplib.SMTPAuthenticationError as e:
        print(f"❌ [EMAIL] Authentication failed: {e}")
        print(f"[EMAIL] Check username/password for {smtp_user}")
        raise
    except smtplib.SMTPConnectError as e:
        print(f"❌ [EMAIL] Connection failed: {e}")
        print(f"[EMAIL] Cannot connect to {smtp_host}:{smtp_port}")
        print(f"[EMAIL] Possible causes: firewall blocking, wrong host/port, network issue")
        raise
    except smtplib.SMTPServerDisconnected as e:
        print(f"❌ [EMAIL] Server disconnected: {e}")
        print(f"[EMAIL] SMTP server closed connection unexpectedly")
        raise
    except TimeoutError as e:
        print(f"❌ [EMAIL] Connection timeout: {e}")
        print(f"[EMAIL] Server {smtp_host}:{smtp_port} is not responding")
        print(f"[EMAIL] Check network connectivity and firewall rules")
        raise
    except OSError as e:
        print(f"❌ [EMAIL] Network error: {e}")
        print(f"[EMAIL] Error code: {e.errno if hasattr(e, 'errno') else 'N/A'}")
        raise
    except Exception as e:
        print(f"❌ [EMAIL] Failed to send email: {type(e).__name__}: {e}")
        raise


def send_success_email(**context):
    """
    Send success notification email.
    
    This function is designed to be called by Airflow PythonOperator.
    It extracts information from the Airflow context.
    """
    print("=" * 60)
    print("[SUCCESS EMAIL] Preparing success notification email...")
    print("=" * 60)
    
    # Extract info from Airflow context
    print("[SUCCESS EMAIL] Extracting information from Airflow context...")
    execution_date = context.get('execution_date', datetime.now())
    dag_id = context.get('dag', {}).dag_id if context.get('dag') else 'Unknown DAG'
    run_id = context.get('run_id', 'Unknown')
    
    print(f"[SUCCESS EMAIL] DAG ID: {dag_id}")
    print(f"[SUCCESS EMAIL] Run ID: {run_id}")
    print(f"[SUCCESS EMAIL] Execution Date: {execution_date}")
    
    # Try to get job_id from XCom
    job_id = None
    try:
        print("[SUCCESS EMAIL] Attempting to retrieve job_id from XCom...")
        job_id = context['ti'].xcom_pull(key='job_id')
        if job_id:
            print(f"[SUCCESS EMAIL] Job ID retrieved: {job_id}")
        else:
            print("[SUCCESS EMAIL] No job_id found in XCom")
    except Exception as e:
        print(f"[SUCCESS EMAIL] Could not retrieve job_id from XCom: {e}")
    
    # Build email content
    subject = f"✅ Pipeline Success: {dag_id}"
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: #4CAF50; color: white; padding: 20px; }}
            .content {{ padding: 20px; }}
            .info {{ background-color: #f9f9f9; padding: 15px; margin: 10px 0; }}
            .footer {{ color: #666; font-size: 12px; padding: 20px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>Pipeline Execution Completed Successfully</h2>
        </div>
        <div class="content">
            <h3>Execution Summary</h3>
            <div class="info">
                <p><strong>DAG ID:</strong> {dag_id}</p>
                <p><strong>Run ID:</strong> {run_id}</p>
                <p><strong>Execution Date:</strong> {execution_date.strftime('%Y-%m-%d %H:%M:%S')}</p>
                {f'<p><strong>Job ID:</strong> {job_id}</p>' if job_id else ''}
            </div>
            <h3>Status</h3>
            <p style="color: green; font-size: 18px;">✅ SUCCESS</p>
            <p>All tasks completed successfully. NiFi ingestion and data processing finished without errors.</p>
        </div>
        <div class="footer">
            <p>This is an automated message from Airflow Data Pipeline.</p>
            <p>Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </body>
    </html>
    """
    
    print("[SUCCESS EMAIL] Invoking email sender...")
    send_email(subject, html_content)
    
    print("=" * 60)
    print("✅ [SUCCESS EMAIL] Success notification completed")
    print("=" * 60)
    return "Email sent successfully"


def send_failure_email(**context):
    """
    Send failure notification email.
    
    This function is designed to be called by Airflow PythonOperator
    when a task fails.
    """
    print("=" * 60)
    print("[FAILURE EMAIL] Preparing failure notification email...")
    print("=" * 60)
    
    # Extract info from Airflow context
    print("[FAILURE EMAIL] Extracting information from Airflow context...")
    execution_date = context.get('execution_date', datetime.now())
    dag_id = context.get('dag', {}).dag_id if context.get('dag') else 'Unknown DAG'
    run_id = context.get('run_id', 'Unknown')
    task_instance = context.get('task_instance')
    
    print(f"[FAILURE EMAIL] DAG ID: {dag_id}")
    print(f"[FAILURE EMAIL] Run ID: {run_id}")
    print(f"[FAILURE EMAIL] Execution Date: {execution_date}")
    
    # Get error info if available
    error_msg = "Unknown error"
    if task_instance:
        try:
            print("[FAILURE EMAIL] Extracting error information from task instance...")
            error_msg = str(task_instance.exception)
            print(f"[FAILURE EMAIL] Error captured: {error_msg[:200]}...")  # First 200 chars
        except Exception as e:
            print(f"[FAILURE EMAIL] Could not extract error message: {e}")
    else:
        print("[FAILURE EMAIL] No task instance available for error details")
    
    # Build email content
    subject = f"❌ Pipeline Failed: {dag_id}"
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: #f44336; color: white; padding: 20px; }}
            .content {{ padding: 20px; }}
            .info {{ background-color: #f9f9f9; padding: 15px; margin: 10px 0; }}
            .error {{ background-color: #ffebee; padding: 15px; margin: 10px 0; border-left: 4px solid #f44336; }}
            .footer {{ color: #666; font-size: 12px; padding: 20px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>Pipeline Execution Failed</h2>
        </div>
        <div class="content">
            <h3>Execution Summary</h3>
            <div class="info">
                <p><strong>DAG ID:</strong> {dag_id}</p>
                <p><strong>Run ID:</strong> {run_id}</p>
                <p><strong>Execution Date:</strong> {execution_date.strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            <h3>Status</h3>
            <p style="color: red; font-size: 18px;">❌ FAILED</p>
            <div class="error">
                <p><strong>Error Message:</strong></p>
                <pre>{error_msg}</pre>
            </div>
            <p>Please check Airflow logs for detailed error information.</p>
        </div>
        <div class="footer">
            <p>This is an automated message from Airflow Data Pipeline.</p>
            <p>Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </body>
    </html>
    """
    
    print("[FAILURE EMAIL] Invoking email sender...")
    send_email(subject, html_content)
    
    print("=" * 60)
    print("✅ [FAILURE EMAIL] Failure notification completed")
    print("=" * 60)
    return "Failure notification sent"


# For standalone testing
print("=" * 60)
print("[TEST MODE] Starting email notification test...")
print("=" * 60)

try:
    send_email(
        subject="Notification Email from Airflow Pipeline",
        html_content="<h1>This is a notification email from Airflow pipeline</h1><p>Data injestion is done successfully.</p>"
    )
    print("\n" + "=" * 60)
    print("✅ [TEST MODE] Test completed successfully")
    print("=" * 60)
except Exception as e:
    print("\n" + "=" * 60)
    print(f"❌ [TEST MODE] Test failed: {e}")
    print("=" * 60)
    raise
