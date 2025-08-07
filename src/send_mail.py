import smtplib 
import os
from email.message import EmailMessage

def send_email_with_attachment(subject, body, filename):
  user = os.environ["EMAIL_USER"]
  password = os.environ["EMAIL_PASSWORD"]
  to = os.environ["EMAIL_RECEIVER"]

msg = EmailMessage ()
msg ["Subject"] = subject
msg ["From"] = user
msg ["To"] = to
msg.set_content(body)

#dosyaeklemekiçin
with open (filename, "rb") as f:
  file_data = f.read()
  file_name = f.name
msg.add_attachment(file_data, maintype ="application", subtype="octet-stream", filename = file_name)

#mailigonder
with smtplib.SMTP_SSL("smtp.office365.com", 587) as smtp:
  smtp.ehlo()
  smtp.starttls()
  smtp.login(user, password)
  smtp.send_message(msg)
