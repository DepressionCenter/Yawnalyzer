import PathKeeper
import pandas as pd
import os
import glob
import datetime as dt
import matplotlib.pyplot as plt


def classify_bins(df, probability_col, thresholds = (0.25,0.5,0.75,0.95)):
    df = df.copy()
    df[probability_col] = pd.to_numeric(df[probability_col], errors="coerce")
    for value in thresholds:
        col_name = f"asleep_{str(value).replace(".","")}"
        df[col_name] = (df[probability_col] >= value).astype(int)
    return df


def convert_unix_to_datetime(df, unix_col):
    df = df.copy()
    df["datetime"]  = pd.to_datetime(df[unix_col], unit="s", utc=True)

    df["datetime_local"] = df["datetime"].dt.tz_convert("America/Detroit")

    return df


def days_covered(df):
    n_bins = len(df)
    total_seconds = n_bins*30

    days = total_seconds/(60*60*24)

    return days

def classify_night(df):
    df = df.copy()

    df["night_id"] = (df["datetime_local"] - pd.Timedelta(hours=12)).dt.date

    return df


def count_nights(df):
    return df["night_id"].nunique()



def process_folder(folder_path):
    results = []

    for file in glob.glob(os.path.join(folder_path, "*.csv")):

        df = pd.read_csv(file)

        df = classify_bins(df, probability_col="sleep_probability")

        df = convert_unix_to_datetime(df, unix_col="timestamp")

        df = classify_night(df)

        participant = df["subject_id"].iloc[0]

        print(f"Number of Missing timestamsp:{df["timestamp"].isna().sum()}")
        print(f"Spacing between bins:{df["timestamp"].diff().describe()}")

        days = days_covered(df)

        nights = count_nights(df)

        results.append({
            "ID": participant,
            "days_covered": days,
            "nights_covered": nights
        })
    return pd.DataFrame(results)

summary_df = process_folder(folder_path=PathKeeper.clean_sleep_classification_file_path)

summary_data ={
    "days_covered_mean" : summary_df["days_covered"].mean(),
    "days_covered_min" : summary_df["days_covered"].min(),
    "days_covered_max" : summary_df["days_covered"].max(),
    "days_covered_mode": summary_df["days_covered"].mode(),
    "days_covered_median" : summary_df["days_covered"].median(),
    "night_covered_mean" : summary_df["nights_covered"].mean(),
    "night_covered_min" : summary_df["nights_covered"].min(),
    "night_covered_max" : summary_df["nights_covered"].max(),
    "night_covered_median" : summary_df["nights_covered"].median(),
    "night_covered_mode": summary_df["nights_covered"].mode()
}

plt.hist(summary_df["nights_covered"])
plt.show()

### Sleep Bout Level

def determine_sleep_session(df, threshold=0.5):
    df = df.copy()

    new_col = f"asleep_{str(threshold).replace('.','')}"

    df.sort_values("datetime_local")

    #Identify change

    df["state_change"] = (df[new_col] != df[new_col].shift(1)).astype(int)

    #Session ID

    df["sleep_session_id"] = df["state_change"].cumsum()

    return df

def summarize_sleep_session(df, threshold=0.5):
    new_col = f"asleep_{str(threshold).replace('.','')}"

    session_summary= (
        df.groupby("sleep_session_id")
        .agg(
            start=("datetime_local","min"),
            end=("datetime_local","max"),
            epoch=("sleep_session_id", "size"),
            asleep=(new_col, "first")
        ).reset_index()
    )

    session_summary["duration_in_min"] = session_summary["epoch"]*0.5
    return session_summary

def filter_sleep_sessions(session_summary, min_minutes=5):
    return session_summary[
        (session_summary["asleep"] ==1) &
        (session_summary["duration_in_min"] >= min_minutes)
    ]

def per_night_sleep_timing(df, threshold=0.5):
    df = determine_sleep_session(df, threshold)

    session = summarize_sleep_session(df, threshold)

    session_filtered = filter_sleep_sessions(session)

    session_filtered = session_filtered.merge(
        df[["sleep_session_id","night_id"]].drop_duplicates(),
        on="sleep_session_id",
        how="left"
    )

    night_summary = (session_filtered.groupby("night_id").agg(
        sleep_onset =("start", "min"),
        final_wake = ("end","max"),
        total_sleep_in_min = ("duration_in_min","sum")
        ).reset_index()
    )

    return night_summary

def additional_night_summarization(night_summary):
    night_summary.copy()

    night_summary["minutes_in_bed"] =(
        (night_summary["final_wake"] - night_summary["sleep_onset"]).dt.total_seconds()/60
    )

    night_summary["sleep_efficency"] = (
        night_summary["total_sleep_in_min"]/night_summary["minutes_in_bed"]
    )

    return night_summary

def compare_thesholds(df, thresholds=(0.25,0.5,0.75,0.95)):
    results={}

    for threshold in thresholds:
        night_summary = per_night_sleep_timing(df, threshold)
        night_summary = additional_night_summarization(night_summary)
        
        results[threshold] = night_summary
    
    return results

def process_folder_with_sleep_metrics(folder_path, probability_col="sleep_probability",unix_col="timestamp"):
    all_results = []

    for file in glob.glob(os.path.join(folder_path, "*.csv")):
        df = pd.read_csv(file)

        df = classify_bins(df,probability_col=probability_col)
        df = convert_unix_to_datetime(df,unix_col="timestamp")
        df = classify_night(df)

        participant = df["subject_id"].iloc[0]

        threshold_results = compare_thesholds(df)

        for threshold, night_df in threshold_results.items():
            if len(night_df) ==0:
                continue
            all_results.append({
                "ID" : participant,
                "theshold": threshold,
                "mean_sleep_minutes": night_df["total_sleep_in_min"].mean(),
                "mean_efficency": night_df["sleep_efficency"].mean(),
                "n_nights": len(night_df)
            })
    return pd.DataFrame(all_results)


def merge_short_sleep_sessions(df, threshold=0.5, max_gap_in_min=120):
    df = df.copy()

    col =f"asleep_{str(threshold).replace('.','')}"
    df=determine_sleep_session(df, threshold)
    sleep_session = summarize_sleep_session(df, threshold)

    short_sleep_sessions = sleep_session[
        (sleep_session["asleep"]==0) &
        (sleep_session["duration_in_min"] <=max_gap_in_min)
    ]["session_id"]

    df.loc[df["sleep_session_id"].isin(short_sleep_sessions), col] = 1

    df["state_change"] = (df[col] != df[col].shift(1)).astype(int)
    df["sleep_session_id"] = df["state_change"].cumsum()

    return df



