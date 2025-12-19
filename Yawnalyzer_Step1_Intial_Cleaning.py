# Copyright © 2025 The Regents of the University of Michigan

#

# This file is part of Yawnalyzer.

#

# This program is free software: you can redistribute it and/or modify

# it under the terms of the GNU General Public License as published by

# the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

#

# This program is distributed in the hope that it will be useful,

# but WITHOUT ANY WARRANTY; without even the implied warranty of

# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the

# GNU General Public License for more details.

#

# You should have received a copy of the GNU General Public License along

# with this program. If not, see <https://www.gnu.org/licenses/>.


import os
import pandas as pd
import re
import PathKeeper  # File with machine-specific paths

vars_used = [
    "df_cognitive_survey",
    "df_cognitive_survey_list",
    "df_hr",
    "df_hr_list",
    "df_sleep",
    "df_sleep_list",
    "df_survey",
    "df_survey_list",
    "df_gait",
    "df_gait_list",
    "df_support_list",
    "df_support",
    "df_asymmetry_list",
    "df_asymmetry",
    "id_value"
    "f",
    "file_type",
    "found_any",
    "individual_file",
    "nodate_file",
    "meta_data",
    "participant",
    "participant_dir",
    "RID",
    "survey_meta_data",
    "processed_participants",
    "submission_date",
    "question_cols",
    "parsed",
    "col",
    "val",
    "main_df",
    "row",
    "df_all_surveys",
    "df_all_surveys_list",
    "df_watch_or_phone_list",
    "quest_name",
    "quest_value",
    "reg_arg",
    "failed_participants",
    "_",
    "vars_used",
    "particpant_id_list",
    "number_pattern"
]

numeric_pattern = re.compile(r"^[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")


def extract_numeric(response):
    numeric_response = numeric_pattern.match(response)

    if not numeric_response:
        return response
    number = float(numeric_response.group(0))
    return int(number) if number.is_integer() else number


df_sleep_list = []
df_gait_list = []
df_hr_list = []
df_survey_list = []
df_cognitive_survey_list = []
df_all_surveys_list = []
df_asymmetry_list = []
df_support_list = []
df_watch_or_phone_list = []
particpant_id_list = []

if os.path.exists(PathKeeper.completed_participant_path):
    with open(PathKeeper.completed_participant_path, "r") as f:
        processed_participants = {line.strip() for line in f if line.strip()}
else:
    processed_participants = set()

if os.path.exists(PathKeeper.incomplete_participant_path):
    with open(PathKeeper.incomplete_participant_path, "r") as f:
        failed_participants = {line.strip() for line in f if line.strip()}
else:
    failed_participants = set()

for participant in os.listdir(PathKeeper.inital_path):
    participant_dir = os.path.join(PathKeeper.inital_path, participant)
    particpant_id_list.append(participant)

    # skip non-folders
    if not os.path.isdir(participant_dir):
        continue

    print("Participant:", participant)

    # --- FOLDER-LEVEL SKIP LOGIC HERE ---
    if participant in processed_participants:
        print(f"Participant {participant}: already processed, skip to next participant")
        continue
    else:
        print(f"Participant {participant}: processing...")
    found_any = False

    if participant == "UMI1003":
        participant_dir = os.path.join(participant_dir, "All")

    for individual_file in os.listdir(participant_dir):
        if individual_file.endswith("phoneSleep.csv") or individual_file.endswith(
            "watchSleep.csv"
        ):
            print(individual_file)
            if individual_file.startswith("combined"):
                continue
            else:
                submission_date, nodate_file = individual_file.split()
                RID, file_type = nodate_file.split("_")
                file_type = file_type.removesuffix(".csv")
                s = pd.read_csv(os.path.join(participant_dir, individual_file))
                s["filename"] = individual_file
                s["ID"] = participant
                s["file_type"] = file_type
                s["submission_date"] = submission_date
                df_watch_or_phone_list.append(s)
                del s
                found_any = True

        if individual_file.endswith("_sleep.csv"):
            print(individual_file)
            if individual_file.startswith("combined"):
                continue
            else:
                submission_date, nodate_file = individual_file.split()
                RID, file_type = nodate_file.split("_")
                file_type = file_type.removesuffix(".csv")
                s = pd.read_csv(os.path.join(participant_dir, individual_file))
                s["filename"] = individual_file
                s["ID"] = participant
                s["file_type"] = file_type
                s["submission_date"] = submission_date
                df_sleep_list.append(s)
                del s
                found_any = True
        elif individual_file.endswith("_heartrate.csv"):
            print(individual_file)
            if individual_file.startswith("combined"):
                continue
            else:
                submission_date, nodate_file = individual_file.split()
                RID, file_type = nodate_file.split("_")
                file_type = file_type.removesuffix(".csv")
                s = pd.read_csv(os.path.join(participant_dir, individual_file))
                s["filename"] = individual_file
                s["ID"] = participant
                s["file_type"] = file_type
                s["submission_date"] = submission_date
                df_hr_list.append(s)
                del s
                found_any = True
        elif individual_file.endswith("_gait.csv"):
            print(individual_file)
            if individual_file.startswith("combined"):
                continue
            else:
                submission_date, nodate_file = individual_file.split()
                RID, file_type = nodate_file.split("_")
                file_type = file_type.removesuffix(".csv")
                s = pd.read_csv(os.path.join(participant_dir, individual_file))
                s["filename"] = individual_file
                s["ID"] = participant
                s["file_type"] = file_type
                s["submission_date"] = submission_date
                df_gait_list.append(s)
                del s
                found_any = True
        elif individual_file.endswith("_asymmetry.csv"):
            print(individual_file)
            if individual_file.startswith("combined"):
                continue
            else:
                submission_date, nodate_file = individual_file.split()
                RID, file_type = nodate_file.split("_")
                file_type = file_type.removesuffix(".csv")
                s = pd.read_csv(os.path.join(participant_dir, individual_file))
                s["filename"] = individual_file
                s["ID"] = participant
                s["file_type"] = file_type
                s["submission_date"] = submission_date
                df_asymmetry_list.append(s)
                del s
                found_any = True
        elif individual_file.endswith("_support.csv"):
            print(individual_file)
            if individual_file.startswith("combined"):
                continue
            else:
                submission_date, nodate_file = individual_file.split()
                RID, file_type = nodate_file.split("_")
                file_type = file_type.removesuffix(".csv")
                s = pd.read_csv(os.path.join(participant_dir, individual_file))
                s["filename"] = individual_file
                s["ID"] = participant
                s["file_type"] = file_type
                s["submission_date"] = submission_date
                df_support_list.append(s)
                del s
                found_any = True
        elif individual_file == "Sleep.csv" or individual_file == "Fatigue.csv":
            print(individual_file)
            file_type = individual_file.removesuffix(".csv")
            s = pd.read_table(os.path.join(participant_dir, individual_file))
            s[["Survey", "Timestamp", "Question1", "Question2", "Question3"]] = s[
                "DATE_KEY,ANSWERS"
            ].str.split(pat=",", expand=True)
            s["filename"] = individual_file
            s["ID"] = participant
            s["file_type"] = file_type
            df_survey_list.append(s)
            del s
            found_any = True
        elif individual_file == "Cognitive.csv":
            file_type = individual_file.removesuffix(".csv")
            s = pd.read_table(os.path.join(participant_dir, individual_file))
            s[
                [
                    "Survey",
                    "Timestamp",
                    "Question1",
                    "Question2",
                    "Question3",
                    "Question4",
                ]
            ] = s["DATE_KEY,ANSWERS"].str.split(pat=",", expand=True)
            s["filename"] = individual_file
            s["ID"] = participant
            s["file_type"] = file_type
            df_cognitive_survey_list.append(s)
            del s
            found_any = True
        # elif individual_file == "all_surveys.csv":
        #     file_type = individual_file.removesuffix(".csv")
        #     s = pd.read_table(os.path.join(participant_dir,individual_file))
        #     s = s.sort_values(by="DATE_KEY,ANSWERS", ascending=True)
        #     s[["Survey", "Timestamp", "Question1", "Question2", "Question3","Question4"]] = s["DATE_KEY,ANSWERS"].str.split(pat=",", expand=True)
        #     s["filename"] = individual_file
        #     s["ID"] = participant
        #     s["file_type"] = file_type
        #     df_cognitive_survey_list.append(s)
        #     del(s)
        #     found_any = True

        else:
            continue
            # print("Not Steps "+filename2)
    if found_any:
        with open(PathKeeper.completed_participant_path, "a") as f:
            f.write(participant + "\n")
        processed_participants.add(participant)
        print(f"Participant {participant}: done, added to registry.")
    else:
        with open(PathKeeper.incomplete_participant_path, "a") as f:
            f.write(participant + "\n")
        failed_participants.add(participant)
        print(f"Participant {participant}: no relevant files, NOT added to registry.")


if df_watch_or_phone_list:
    watch_or_phone_sleep_df = pd.concat(df_watch_or_phone_list, ignore_index=True)
    watch_or_phone_sleep_df = watch_or_phone_sleep_df.sort_values(
        by=["ID", "Start"], ascending=[True, True]
    ).reset_index(drop=True)

    new_rows = []
    for id_value, data in watch_or_phone_sleep_df.groupby("ID", sort=False):
        data = data.sort_values("Start")
        current_data = data.iloc[0].copy()
        start_value = current_data["Start"]
        end_value = current_data["End"]

        for _, row in data.iloc[1:].iterrows():
            if row["Start"] - end_value <= 30:
                end_value = max(end_value, row["End"])
            else:
                current_data["Start"] = start_value
                current_data["End"] = end_value
                new_rows.append(current_data)

                current_data = row.copy()
                start_value = current_data["Start"]
                end_value = current_data["End"]

        current_data["Start"] = start_value
        current_data["End"] = end_value
        new_rows.append(current_data)

        df_sleep = pd.DataFrame(new_rows)


if df_sleep_list:
    df_sleep = pd.concat(df_sleep_list, ignore_index=True)
    df_sleep = df_sleep.drop_duplicates(subset=["Start"], keep="first")

if df_hr_list:
    df_hr = pd.concat(df_hr_list, ignore_index=True)
    df_hr = df_hr.drop_duplicates(subset=["Timestamp"], keep="first")

if df_survey_list:
    for survey in df_survey_list:
        if "Question4" not in survey.columns:
            survey["Question4"] = pd.NA
    df_survey = pd.concat(df_survey_list, ignore_index=True)
    df_survey = df_survey.drop_duplicates(subset=["Timestamp"], keep="first")

if df_cognitive_survey_list:
    df_cognitive_survey = pd.concat(df_cognitive_survey_list, ignore_index=True)
    df_cognitive_survey = df_cognitive_survey.drop_duplicates(subset=["Timestamp"], keep="first")
if df_gait_list:
    df_gait = pd.concat(df_gait_list, ignore_index=True)
    df_gait = df_gait.drop_duplicates(subset=["Timestamp"], keep="first")
if df_asymmetry_list:
    df_asymmetry = pd.concat(df_asymmetry_list, ignore_index=True)
    df_asymmetry = df_asymmetry.drop_duplicates(subset=["Timestamp"], keep="first")

if df_support_list:
    df_support = pd.concat(df_support_list, ignore_index=True)
    df_support = df_support.drop_duplicates(subset=["Timestamp"], keep="first")

df_sleep = df_sleep if "df_sleep" in locals() else pd.DataFrame()
df_hr = df_hr if "df_hr" in locals() else pd.DataFrame()
df_survey = df_survey if "df_survey" in locals() else pd.DataFrame()
df_cognitive_survey = (
    df_cognitive_survey if "df_cognitive_survey" in locals() else pd.DataFrame()
)
df_gait = df_gait if "df_gait" in locals() else pd.DataFrame()
df_asymmetry = df_asymmetry if "df_asymmetry" in locals() else pd.DataFrame()
df_support = df_support if "df_support" in locals() else pd.DataFrame()

df_all_surveys = pd.DataFrame()

meta_data = ["ID", "submission_date", "file_type"]
survey_meta_data = ["ID", "file_type"]


if not df_cognitive_survey.empty and not df_survey.empty:
    df_all_surveys = pd.concat([df_cognitive_survey, df_survey], axis=0)
    # if df_all_surveys_list:
    #     df_all_surveys_list = pd.concat(df_all_surveys_list, ignore_index=True)

if not df_sleep.empty:
    df_sleep = df_sleep[
        meta_data + [col for col in df_sleep.columns if col not in meta_data]
    ]
    df_sleep.to_csv(
        os.path.join(PathKeeper.merged_data_path, "sleep_collapsed.csv"),
        index_label="index_1",
    )
if not df_hr.empty:
    df_hr = df_hr[meta_data + [col for col in df_hr.columns if col not in meta_data]]
    df_hr.to_csv(
        os.path.join(PathKeeper.merged_data_path, "hr_collapsed.csv"),
        index_label="index_1",
    )
if not df_survey.empty:
    df_survey = df_survey[
        survey_meta_data
        + [col for col in df_survey.columns if col not in survey_meta_data]
    ]

    df_sleep_survey = df_survey[df_survey["file_type"] == "Sleep"]
    df_sleep_survey = df_sleep_survey.rename(
        columns={"Question1": "Sleep01", "Question2": "Sleep02", "Question3": "Sleep03"}
    )

    df_fatigue_survey = df_survey[df_survey["file_type"] == "Fatigue"]
    df_fatigue_survey = df_fatigue_survey.rename(
        columns={
            "Question1": "Physical_Fatigue",
            "Question2": "Brain_Fatigue",
            "Question3": "Sleepiness",
        }
    )

    df_sleep_survey.to_csv(
        os.path.join(PathKeeper.merged_data_path, "sleep_survey_collapsed.csv"),
        index_label="index_1",
    )
    df_fatigue_survey.to_csv(
        os.path.join(PathKeeper.merged_data_path, "fatigue_survey_collapsed.csv"),
        index_label="index_1",
    )
if not df_cognitive_survey.empty:
    df_cognitive_survey = df_cognitive_survey[
        survey_meta_data
        + [col for col in df_cognitive_survey.columns if col not in survey_meta_data]
    ]
    df_cognitive_survey.rename(
        columns={
            "Question1": "Cognitive01",
            "Question2": "Cognitive02",
            "Question3": "Pain",
            "Question4": "Depression",
        }
    ).to_csv(
        os.path.join(PathKeeper.merged_data_path, "cog_collapsed.csv"),
        index_label="index_1",
    )

if not df_gait.empty:
    df_gait = df_gait[
        meta_data + [col for col in df_gait.columns if col not in meta_data]
    ].rename(columns={"Value": "Balance"})
    df_gait.to_csv(
        os.path.join(PathKeeper.merged_data_path, "gait_collapsed.csv"),
        index_label="index_1",
    )

if not df_asymmetry.empty:
    df_asymmetry = df_asymmetry[
        survey_meta_data + [col for col in df_asymmetry.columns if col not in meta_data]
    ]
    df_asymmetry.to_csv(
        os.path.join(PathKeeper.merged_data_path, "asymmetry_collapsed.csv"),
        index_label="index_1",
    )

if not df_support.empty:
    df_support = df_support[
        survey_meta_data + [col for col in df_support.columns if col not in meta_data]
    ].rename(columns={"Value": "Percentage"})
    df_support.to_csv(
        os.path.join(PathKeeper.merged_data_path, "double_support_collapsed.csv"),
        index_label="index_1",
    )

if not df_all_surveys.empty:
    df_all_surveys = df_all_surveys[
        survey_meta_data
        + [col for col in df_all_surveys.columns if col not in survey_meta_data]
    ].drop(columns="DATE_KEY,ANSWERS")
    question_cols = ["Question1", "Question2", "Question3", "Question4"]
    rows_fulltext = []
    for _, row in df_all_surveys.iterrows():
        main_df = {
            "ID": row["ID"],
            "file_type": row["file_type"],
            "Survey": row["Survey"],
            "Timestamp": row["Timestamp"],
            "filename": row["filename"],
        }
        parsed = {}
        for col in question_cols:
            val = row.get(col)
            if pd.isna(val):
                continue
            if ":" in val:
                quest_name, quest_value = val.split(":", 1)
                quest_name = quest_name.strip()
                quest_value = quest_value.strip()
                parsed[quest_name] = extract_numeric(quest_value)
            else:
                parsed[quest_name] = val
        rows_fulltext.append({**main_df, **parsed})
    df_all_surveys = pd.DataFrame(rows_fulltext)
    df_all_surveys.to_csv(
        os.path.join(PathKeeper.merged_data_path, "all_survey_collapsed.csv"),
        index_label="index_1",
    )
    del rows_fulltext


for variable in vars_used:
    if variable in locals():
        del locals()[variable]

del variable
