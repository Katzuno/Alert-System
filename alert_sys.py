from gdcb_azure_helper import MSSQLHelper
import pandas as pd
import numpy as np
import time
import smtplib
import os
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Alert:
    def __init__(self):
        self.sql_eng = MSSQLHelper()
        self.statuses = self.sql_eng.ReadTable("CarsStatus", caching=False)
        cfg_file = open("mail_config.txt")
        self.mail_data = json.load(cfg_file)
        cfg_file.close()

    def updateStatuses(self):
        self.statuses = self.sql_eng.ReadTable("CarsStatus", caching=False)

    def getOwnerEmail(self, acc_id):
        email = self.sql_eng.Select("SELECT Adresa_email FROM Users WHERE Flota_detinuta = " + str(acc_id))
        email = email['Adresa_email'][0]
        return email

    def getChanges(self, oldStatuses):
        df_stacked = (self.statuses != oldStatuses).stack()
        changed = df_stacked[df_stacked]
        changed.index.names = ['CarID', 'Status']
        differences = np.where(self.statuses != oldStatuses)
        #        print (differences)
        changed_from = self.statuses.values[differences]
        changed_to = oldStatuses.values[differences]
        changes = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=changed.index)
        car_ids = list(changes.index.labels[0])
        return {'changes_df': changes.copy(), 'cars': car_ids}

    def sendMailTo(self, car_id, alerta="None"):
        print("Sending mail to owner of " + str(car_id))
        car_details = self.sql_eng.Select("SELECT AccountID, Name, Description FROM Cars WHERE ID = " + str(car_id))
        msg = MIMEMultipart()
        password = self.mail_data['mail_pass']
        msg['From'] = "GoDrive Carbox "
        msg['To'] = self.getOwnerEmail(car_details['AccountID'][0])
        msg['Subject'] = "GoDrive Carbox - Alerta masina " + car_details['Name'][0]
        # Create the body of the message (a plain-text and an HTML version).
        # text = "Test"
        text = "<html> <body> Va informam ca masina dumneavoastra <strong>" + car_details['Name'][0] + "</strong>" + \
               " cu descrierea <em>" + car_details['Description'][0] + "</em> are urmatoarea <strong>alerta</strong>:" \
                                                                       " <em style='color: red'>" + alerta + "</em>" \
               + "<footer>" \
                 "<br/><br/><hr/> Acesta este un mesaj de alerta generat automat cu scopul de a va informa in privinta " \
                 "functionarii masinii dumneavoastra. <br/>" \
               + "Va rugam sa nu faceti reply la acest email! </footer></body></html>"

        part1 = MIMEText(text, 'html')
        msg.attach(part1)
        print("Message created")
        server = smtplib.SMTP(self.mail_data['mail_server'], self.mail_data['mail_port'])
        print("server created, trying to create connection")
        server.connect(self.mail_data['mail_server'], self.mail_data['mail_port'])
        print("SMTP connected")
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(self.mail_data['mail_sender'], password)
        print("Logged in, trying to send mail")
        server.sendmail(self.mail_data['mail_sender'], msg['To'], msg.as_string())
        print("Mail sent")
        server.quit()

    def run(self):
        oldStatuses = self.statuses.copy()
        counter = 0
        while True:
            counter = counter + 1
            print('--- Loop number ' + str(counter) + ' ---')
            time.sleep(1)
            self.updateStatuses()
            changes = self.getChanges(oldStatuses)
            if changes['changes_df'].empty:
                print('No changes found')
                # print (self.statuses)
            else:
                print('Change found... Alerting... Please wait')
                iterator = 0
                for id in changes['cars']:
                    alerta = changes['changes_df']['to'][iterator]
                    print(alerta)
                    self.sendMailTo(id + 1, alerta)
                    iterator = iterator + 1

            oldStatuses = self.statuses.copy()

    def getStatuses(self):
        return self.statuses

    def printStatuses(self):
        print(self.statuses)


if __name__ == "__main__":
    alert = Alert()
    alert.printStatuses()
    old = alert.getStatuses().copy()
    old.at[56, 'Status'] = "ALERT"
    old.at[2, 'Status'] = "GOOD"
    changes = alert.getChanges(old)
    print(changes)
    alert.run()
