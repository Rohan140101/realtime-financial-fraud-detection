import requests
import random
from datetime import datetime
import argparse
import threading
import time

post_api_url = "http://localhost:8080/events"
events = 0
fraud_bursts = 0
errors = 0
accounts = ["ACC-001", "ACC-002", "ACC-003", "ACC-004", "ACC-005", 
            "ACC-006", "ACC-007", "ACC-008", "ACC-009", "ACC-010",
            "ACC-011", "ACC-012", "ACC-013", "ACC-014", "ACC-015", 
            "ACC-016", "ACC-017", "ACC-018", "ACC-019", "ACC-020"]

merchants = ["MERCH-041", "MERCH-042", "MERCH-043", "MERCH-044", "MERCH-045", 
            "MERCH-046", "MERCH-047", "MERCH-048", "MERCH-049", "MERCH-050",]

atms = ["ATM-001", "ATM-002", "ATM-003", "ATM-004", "ATM-005"]
currencies = ['USD', 'EUR', 'GBP']
currency_sources = ["cash", "check", "wire"]
instruments = ["stock", "bond", "etf", "crypto"]
tickers =  ["AAPL", "BTC", "GOOG", "META"]
txn_types = ['payment', 'transfer', 'withdrawal', 'deposit', 'invest']
lock = threading.Lock()


def generate_txn(acct_id=None):
    txn_type = random.choices(txn_types, weights=[35, 25, 20, 10, 10], k=1)[0]
    transaction = dict()
    transaction['payload'] = dict()
    transaction['timestamp'] = datetime.now().isoformat() + "Z"
    transaction['type'] = txn_type
    if not acct_id:
        account_id = random.choice(accounts)
    else:
        account_id = acct_id
    transaction['payload']['currency'] = random.choice(currencies)

    if txn_type == "payment":
        transaction['payload']['accountId'] = account_id
        transaction['payload']['amount'] = round(random.uniform(10, 5000), 2)
        transaction['payload']['merchantId'] = random.choice(merchants)
    elif txn_type == "transfer":
        transaction['payload']['sender'] = account_id
        receiver = random.choice(accounts)
        while receiver == account_id:
            receiver = random.choice(accounts)
        transaction['payload']['receiver'] = receiver
        transaction['payload']['amount'] = round(random.uniform(50, 20000), 2)
    elif txn_type == "withdrawal":
        transaction['payload']['accountId'] = account_id
        transaction['payload']['amount'] = round(random.uniform(20, 2000), 2)
        transaction['payload']['atmId'] = random.choice(atms)
    elif txn_type == "deposit":
        transaction['payload']['accountId'] = account_id
        transaction['payload']['amount'] = round(random.uniform(100, 10000), 2)
        transaction['payload']['source'] = random.choice(currency_sources)
    elif txn_type == "invest":
        transaction['payload']['accountId'] = account_id
        transaction['payload']['amount'] = round(random.uniform(500, 50000), 2)
        transaction['payload']['instrument'] = random.choice(instruments)
        transaction['payload']['ticker'] = random.choice(tickers)
    return transaction




def send_txn():
    global events, errors
    while True:
        with lock:
            txn = generate_txn()
            try:
                response = requests.post(post_api_url, json=txn)
                if response.status_code == 202:
                    events += 1
                else:
                    errors += 1
            except requests.exceptions.RequestException:
                errors += 1
        time.sleep(1/rate)


def generate_fraud():
    global events, errors, fraud_bursts
    while True:
        with lock:
            acct_id = random.choice(accounts)
            print(f"[FRAUD INJECTION] Burst of 6 events for account {acct_id}")
            for i in range(6):
                txn = generate_txn(acct_id)
                try:
                    response = requests.post(post_api_url, json=txn)
                    if response.status_code == 202:
                        events += 1
                    else:
                        errors += 1
                except requests.exceptions.RequestException:
                    errors += 1
            fraud_bursts += 1
        time.sleep(30)


def progress_reporting():
    global events, fraud_bursts, errors
    start_time = time.perf_counter()
    while True:
        time.sleep(10)
        with lock:
            elapsed = time.perf_counter() - start_time
            print(f"[STATS] Sent: {events} events | Fraud bursts: {fraud_bursts} | Errors: {errors} | Uptime: {elapsed}s")    

parser = argparse.ArgumentParser()

parser.add_argument(
    "--rate",
    default=5,
    help="Rate of Sending Events"
)

args = parser.parse_args()
rate = args.rate

t1 = threading.Thread(target=send_txn, daemon=True)
# t2 = threading.Thread(target=generate_fraud, daemon=True)
t3 = threading.Thread(target=progress_reporting, daemon=True)

t1.start()
# t2.start()
t3.start()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print(f"\n[SHUTDOWN] Final stats - Sent: {events} | Fraud bursts: {fraud_bursts} | Errors: {errors}")