# SmartWorks Sub Vendor Accrual Automation

"""This script automates the accrual process for subvendors in SmartWorks. The automation aims to streamline vendor payments and minimize errors through effective data management and reporting."""

import datetime

class VendorAccrual:
    def __init__(self, vendor_id, amount):
        self.vendor_id = vendor_id
        self.amount = amount
        self.accrual_date = datetime.datetime.utcnow()

    def create_accrual(self):
        # Logic to create an accrual in the system
        print(f'Accrual created for vendor {self.vendor_id} for amount {self.amount} on {self.accrual_date}')

if __name__ == '__main__':
    # Example usage
    vendor_accrual = VendorAccrual(vendor_id='V001', amount=1000.00)
    vendor_accrual.create_accrual()