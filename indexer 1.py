import os
import json
import sqlite3
import logging
from cryptography.fernet import Fernet

logging.basicConfig(filename="etl.log", level=logging.INFO, format="%(asctime)s - %(message)s")

encryption_key = b'G8gFPBD-k2O55wjyETEt-Kb4eujrxvJ0cvLL7biESUQ='
cipher = Fernet(encryption_key)

def get_user_input():
    print("Please provide the path to the folder containing your JSON files:")
    folder_path = input("Enter folder path: ").strip()
    if not os.path.isdir(folder_path):
        print(f"Error: {folder_path} is not a valid folder. Please try again.")
        exit()
    return folder_path

def extract_ledger_files(folder_path):
    transaction_data = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    transaction_data.append(data)
                logging.info(f"Successfully extracted: {filename}")
            except Exception as e:
                logging.error(f"Error reading file {filename}: {e}")
    return transaction_data

def decrypt_transactions(data, cipher):
    decrypted_transactions = []
    for record in data:
        try:
            decrypted_record = {
                "transaction_id": record["transaction_id"],
                "sender": cipher.decrypt(record["sender"].encode()).decode(),
                "receiver": cipher.decrypt(record["receiver"].encode()).decode(),
                "amount": float(cipher.decrypt(record["amount"].encode()).decode()),
                "timestamp": record["timestamp"]
            }
            decrypted_transactions.append(decrypted_record)
        except KeyError as e:
            logging.error(f"Missing key in record: {e}")
        except Exception as e:
            logging.error(f"Error decrypting record: {e}")
    return decrypted_transactions

def load_to_database_and_save(data, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT UNIQUE,
        sender TEXT,
        receiver TEXT,
        amount REAL,
        timestamp TEXT
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaction_id ON Transactions (transaction_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sender ON Transactions (sender)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_receiver ON Transactions (receiver)")


    final_data = []
    for record in data:
        try:
            cursor.execute("""
            INSERT INTO Transactions (transaction_id, sender, receiver, amount, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """, (record["transaction_id"], record["sender"], record["receiver"], record["amount"], record["timestamp"]))
            final_data.append(record)
        except sqlite3.IntegrityError:
            logging.warning(f"Duplicate transaction ID: {record['transaction_id']}")
        except Exception as e:
            logging.error(f"Error inserting record: {e}")

    conn.commit()
    conn.close()


    output_file = "processed_transactions.json"
    with open(output_file, 'w') as outfile:
        json.dump(final_data, outfile, indent=4)
    print(f"Final processed file saved to: {output_file}")
    logging.info(f"Final processed file saved to: {output_file}")

# ETL process
def etl_process():
    folder_path = get_user_input()

    print("Extracting data...")
    raw_data = extract_ledger_files(folder_path)

    # Translate data
    print("Translating data...")
    decrypted_data = decrypt_transactions(raw_data, cipher)

    # Load data into database and save to file
    print("Loading data into the database and saving to file...")
    load_to_database_and_save(decrypted_data, "transaction_database.db")

    print("ETL process completed successfully.")

# Run the ETL process
etl_process()
