import smtplib
from email.mime.text import MIMEText

smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_user = 'kipngenogregory@gmail.com'
smtp_password = 'ysem rfan komx vaiq'
from_addr = 'kipngenogregory@gmail.com'
to_addr = 'kipngenogregory@gmail.com'
subject = 'Airflow SMTP Test'
body = 'This is a test email from Airflow SMTP troubleshooting.'

msg = MIMEText(body)
msg['Subject'] = subject
msg['From'] = from_addr
msg['To'] = to_addr

try:
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.ehlo()
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.sendmail(from_addr, [to_addr], msg.as_string())
    server.quit()
    print('Email sent successfully!')
except Exception as e:
    print('Failed to send email:', e)
