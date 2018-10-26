from gdcb_azure_helper import MSSQLHelper
import pandas as pd
import numpy as np
import time


class Alert:
    def __init__(self):
        self.sql_eng = MSSQLHelper()
        self.statuses = self.sql_eng.ReadTable("CarsStatus", False)

    def updateStatuses(self):
        self.statuses = self.sql_eng.ReadTable("CarsStatus", False)

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

    def sendMailTo(self, car_id):
        print("Sending mail to owner of " + str(car_id))

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
                for id in changes['cars']:
                    self.sendMailTo(id)
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
