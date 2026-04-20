import pandas as pd
import os
import PathKeeper  # File containing machine-specific paths


def to_standardized_timezone(time_column, time_zone) :
    #Convert time column to datetime
    time_column = pd.to_datetime(time_column, errors="coerce", utc=True, unit="ns")
    #If there is no Timezone set, set timezone
    if getattr(time_column.dt, "tz", None) is None:
        return time_column.dt.tz_localize(time_zone)
    # Convert the datetime to the desired timezone.
    return time_column.dt.tz_convert(time_zone)



hr_df = pd.read_csv(os.path.join(PathKeeper.merged_data_path,"hr_collapsed.csv"), parse_dates=["TimestampISO"], index_col="index_1", dtype={
  "ID": "string",
  "submission_date": "string",
  "file_type": "string",
  "filename": "string",
  "HR": "float64",
  "Timestamp":  "float64",
  "filename": "string",
  "TimestampISO": "string",
 }
)

accel_df = pd.read_csv(os.path.join(PathKeeper.merged_data_path,"accel_collapsed.csv"), index_col=["index_1"],engine="pyarrow", dtype={
    "ID":"string",
    "submission_date": "string",
    "file_type":"string",
    "x": "float64",
    "y": "float64",
    "z": "float64",
    "TimestampUnix": "float64"
}, parse_dates=["Time", "TimestampISO"])


hr_df["ID"] = hr_df["ID"].astype(str).str.strip()
accel_df["ID"] = accel_df["ID"].astype(str).str.strip()

shared_ids = sorted(set(hr_df["ID"]).intersection(accel_df["ID"]))

ids_ran = []

shared_ids2 = list(set(shared_ids)-set(ids_ran))

# for participant in shared_ids:
for participant in shared_ids2:

    print(participant)
    this_hr_df = hr_df[hr_df["ID"] == participant].copy()
    this_accel_df = accel_df[accel_df["ID"] == participant].copy()
    #this_hr_df = this_hr_df[["ID","Timestamp","HR"]]

    # Acceleration Processing
    this_accel_df["Time"] = pd.to_datetime(this_accel_df.get("Time"), errors="coerce", utc=True)
    new_unix = this_accel_df["Time"].apply(
        lambda x:int(x.timestamp()) if pd.notnull(x) else pd.NA
    )
    this_accel_df["Timestamp"] = (this_accel_df["TimestampUnix"].fillna(
        new_unix
    )
    )
    accel_name = participant + "_acceleration.txt"

    this_accel_df=this_accel_df[["Timestamp","x","y","z"]].sort_values(by="Timestamp")


    this_accel_df.to_csv(
        os.path.join(PathKeeper.raw_sleep_classification_file_path,"motion",accel_name),
        sep=" ",
        index=False,
        header=False,
        na_rep="NA"
        )
    # Heart Rate
    this_hr_df["TimestampISO"] = pd.to_datetime(this_hr_df.get("TimestampISO"), errors="coerce", utc=True)
    hr_unix = this_hr_df["TimestampISO"].apply(
        lambda x:int(x.timestamp()) if pd.notnull(x) else pd.NA

    )
    this_hr_df["Timestamp"] =(this_hr_df["Timestamp"].fillna(
        hr_unix
    )
    )

    ## Select only necessary Columns of data
    this_hr_df_minimal= this_hr_df[["Timestamp", "HR"]]

    this_hr_df_minimal["Timestamp"] = pd.to_datetime(this_hr_df_minimal["Timestamp"], unit="s")

    this_hr_df2_interprolated = (
        this_hr_df_minimal.sort_values("Timestamp")
        .set_index("Timestamp")[["HR"]]
        .resample("1s")
        .mean()
    )

        
    this_hr_df2_interprolated["HR"] = this_hr_df2_interprolated["HR"].interpolate(method="linear", limit_area="inside")
    this_hr_df2_interprolated["Timestamp"] = this_hr_df2_interprolated.index.view("int64")//10**9
    #this_hr_df2_interprolated = this_hr_df2_interprolated.reset_index()
    this_hr_df2_interprolated = this_hr_df2_interprolated[["Timestamp","HR"]]

    hr_name = participant + "_heartrate.txt"

    this_hr_df2_interprolated.to_csv(
        os.path.join(PathKeeper.raw_sleep_classification_file_path,"heart_rate",hr_name),
        sep=",",
        index=False,
        header=False,
        na_rep="NA"
        )

    ids_ran.append(participant)

id_df = pd.DataFrame(ids_ran, columns=["ID"])

id_df.to_csv(os.path.join(PathKeeper.raw_sleep_classification_file_path,"IDs_prepped_for_classification.csv"))
   








