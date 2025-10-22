#! /usr/bin/python3

import argparse
import ciso8601
import numpy as np
import os
import pandas as pd

# Modified version of clean.py to use local emails.csv file

def clean_enron(s):
    """
    Cleans Enron user names.
    """
    if type(s) is str:
        s = s.replace("[", "")
        s = s.replace("]", "")
        s = s.replace("\'", "")
        s = s.replace("\"", "")
        s = s.replace(" ", "")

        return s
    else:
        return ""


def clean_multiple(s, clean_fn, delimiter):
    """
    Cleans multiple user names.

    s - the user names in a string.
    clean_fn - the function to clean individual user names.
    delimiter - the string that marks the end of a user name.
    """
    if type(s) is str:
        s = [clean_fn(r) for r in s.split(delimiter)]

        return s
    else:
        return []


def convert_time(s):
    """
    Converts string times to a standard float datetime.
    """
    try:
        # Handle the format: 2001-05-14 23:39:00+00:00
        if '+' in s:
            s = s.split('+')[0]  # Remove timezone
        dt = ciso8601.parse_datetime(s)
        return int(np.round(dt.timestamp()))
    except:
        return -1


def expand(row):
    """
    Expands a tuple with multiple receivers into a data frame,
    where the sender and time are constant and each row has a single receiver.
    """
    r = np.empty((len(row[1]), len(row)), dtype=object)
    r[:] = row
    for i in range(r.shape[0]):
        r[i, 1] = row[1][i]
    return r


def clean(df, start, end, clean_fn, delimiter):
    """
    Cleans the dataset. Ensures that sender and receiver information is set, and
    times are between start and end. Then factorizes the senders and receivers.
    Returns a cleaned dataframe.
    """
    print("initial: ", df.shape)
    df.sender = df.sender.apply(clean_fn)
    df.receiver = df.receiver.apply(lambda x:
                                    clean_multiple(x, clean_fn, delimiter))
    df.submit = df.submit.apply(convert_time)

    reorder_cols = ["sender", "receiver", "submit", "cc", "bcc"]
    df = df[reorder_cols].to_numpy()
    df = np.concatenate([expand(df[i, :]) for i in range(df.shape[0])], axis=0)
    print("expansion: ", df.shape)

    df = pd.DataFrame(df, columns=reorder_cols)
    sender_correct = df.sender.apply(lambda x: x != "" and x.lower() != "nan")
    print("senders: ", sum(sender_correct), len(sender_correct))
    receiver_correct = df.receiver.apply(
        lambda x: x != "" and x.lower() != "nan")
    print("receivers: ", sum(receiver_correct), len(receiver_correct))
    time_correct = df.submit.apply(lambda x: start <= x and x <= end)
    print("time: ", sum(time_correct), len(time_correct))
    all_correct = sender_correct & receiver_correct & time_correct
    df = df[all_correct]
    print("final: ", df.shape)

    stacked = df[["sender", "receiver"]].stack()
    sender_receiver, user_key = stacked.factorize()
    print("users: ", np.unique(user_key).shape)
    df[["sender", "receiver"]] = pd.Series(
        sender_receiver, index=stacked.index).unstack()
    clean_cols = ["sender", "receiver", "submit"]

    user_key = pd.DataFrame(user_key, columns=["user"])
    df = df[clean_cols]
    df.sort_values(by=["submit", "sender", "receiver"], inplace=True)

    return df, user_key


def process_local(data_path, emails_file):
    """
    Processes the local emails.csv file into a more usable form.
    """
    
    dataset_path = os.path.join(data_path, "enron")
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)

    clean_path = os.path.join(dataset_path, "clean.csv")
    if not os.path.exists(clean_path):
        print(f"Processing: {emails_file}...")

        # Read the emails.csv file
        raw_df = pd.read_csv(emails_file)
        
        # Rename columns to match expected format
        rename_dict = {
            "From": "sender",
            "To": "receiver", 
            "Date": "submit",
            "X-cc": "cc",
            "X-bcc": "bcc"
        }
        
        # Only keep columns that exist
        available_cols = {k: v for k, v in rename_dict.items() if k in raw_df.columns}
        raw_df = raw_df[list(available_cols.keys())]
        raw_df.rename(columns=available_cols, inplace=True)
        
        # Add missing columns with empty values
        for col in ["cc", "bcc"]:
            if col not in raw_df.columns:
                raw_df[col] = ""
        
        # Enron time range: July 16, 1985 to December 3, 2001
        start = 490338000  # July 16, 1985
        end = 1007337600   # December 3, 2001
        
        clean_df, user_key = clean(
            raw_df, start, end, clean_enron, ",")
        print("done.")

        clean_df.to_csv(clean_path, index=False)

        user_key_path = os.path.join(dataset_path, "users.csv")
        user_key.to_csv(user_key_path, index=False)
        
        print(f"Created: {clean_path}")
        print(f"Created: {user_key_path}")
        print(f"Processed {len(clean_df)} messages from {len(user_key)} users")
    else:
        print(f"Clean data already exists: {clean_path}")


def main(data_path, emails_file):
    """
    Processes the local emails.csv file.
    """
    process_local(data_path, emails_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Local Metadata Cleaner", 
        description="This code cleans the local Enron emails.csv file.")
    parser.add_argument(
        "data_path", type=str, help="Data path to store generated data files.")
    parser.add_argument(
        "emails_file", type=str, help="Path to the emails.csv file.")
    args = parser.parse_args()

    data_path = os.path.abspath(args.data_path)
    emails_file = os.path.abspath(args.emails_file)

    main(data_path, emails_file)
