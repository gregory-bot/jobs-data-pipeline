import smtplib

server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login('kipngenogregory@gmail.com', 'lvmg auwi qaby fznf')
server.sendmail('kipngenogregory@gmail.com', 'kipngenogregory@gmail.com', 'Subject: Test\n\nThis is a test email from WSL.')
server.quit()