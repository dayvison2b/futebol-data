import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header

recipient_emails = ["dayvisoncordeiro2001@gmail.com", "gabrielmaia.amorim01@gmail.com", "felipegf600@gmail.com"]

with open('settings/email_password.txt', 'r') as email_password_file:
    email_password = email_password_file.read()
    
SMTP_SERVER = "smtp.gmail.com"
SENDER_EMAIL = "dayvisoncordeiro2001@gmail.com"
SMTP_PASSWORD = email_password
PORT = 465

def send_email(subject, message, recipient_email=recipient_emails):
    msg = MIMEMultipart()
    msg['From'] = str(Header(f'FUTEBOL-DATA <{SENDER_EMAIL}>'))
    msg['To'] = ", ".join(recipient_email) if isinstance(recipient_email, list) else recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(message, 'plain'))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, PORT, context=context) as server:
            server.login(SENDER_EMAIL, SMTP_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipient_email, msg.as_string())
        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False