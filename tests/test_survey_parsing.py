# Copyright © 2025 The Regents of the University of Michigan
#
# This file is part of Yawnalyzer.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program. If not, see <https://www.gnu.org/licenses/>.

import csv
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CLEANING_SCRIPT = REPOSITORY_ROOT / "Yawnalyzer_Step1_Intial_Cleaning.py"


class SurveyParsingTests(unittest.TestCase):
    def test_trailing_empty_answer_does_not_erase_previous_question(self):
        self.assert_previous_question_is_preserved("")

    def test_trailing_whitespace_answer_does_not_erase_previous_question(self):
        self.assert_previous_question_is_preserved("   ")

    def test_trailing_unlabelled_answer_does_not_erase_previous_question(self):
        self.assert_previous_question_is_preserved("unlabelled")

    def assert_previous_question_is_preserved(self, trailing_answer):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            input_dir = temp / "input"
            participant_dir = input_dir / "UMI1003"
            output_dir = temp / "output"
            participant_dir.mkdir(parents=True)
            output_dir.mkdir()

            # The combined export has one row per answer set and an empty
            # fourth answer field on the three-question sleep survey.
            (participant_dir / "combined_all_surveys.csv").write_text(
                "umid,DATE_KEY,ANSWERS,,,,\n"
                "UMI1003,Sleep_Survey,2025-03-10T08:00:00-04:00,"
                f"Sleep01: 3,Sleep02: 4,Sleep03: 2,{trailing_answer}\n"
                "UMI1003,Memory_Cognitive,2025-03-10T09:00:00-04:00,"
                "Cog01: 5,Cog02: 6,Cog03: 7,Cog04: 8\n",
                encoding="utf-8",
            )
            (participant_dir / "2025-03-10 RID_watchAccel.csv").write_text(
                "Time,TimestampISO\n1000000000,2025-03-10T08:00:00-04:00\n",
                encoding="utf-8",
            )
            (participant_dir / "2025-03-10 RID_accel.csv").write_text(
                "Time,TimestampISO\n2000000000,2025-03-10T08:01:00-04:00\n",
                encoding="utf-8",
            )
            (participant_dir / "2025-03-10 RID_phoneSleep.csv").write_text(
                "Start,End,StartISO,EndISO\n"
                "1000,1100,2025-03-10T08:00:00-04:00,"
                "2025-03-10T08:01:00-04:00\n",
                encoding="utf-8",
            )
            (participant_dir / "2025-03-10 RID_sleep.csv").write_text(
                "Start,End,StartISO,EndISO\n"
                "2000,2100,2025-03-10T09:00:00-04:00,"
                "2025-03-10T09:01:00-04:00\n",
                encoding="utf-8",
            )

            paths_module = temp / "PathKeeper.py"
            paths_module.write_text(
                "inital_path = " + repr(str(input_dir)) + "\n"
                "completed_participant_path = "
                + repr(str(temp / "completed.txt"))
                + "\n"
                "incomplete_participant_path = "
                + repr(str(temp / "incomplete.txt"))
                + "\n"
                "merged_data_path = " + repr(str(output_dir)) + "\n",
                encoding="utf-8",
            )

            # Run an isolated copy so Python cannot import a repository-local
            # PathKeeper.py that points to real study data.
            isolated_script = temp / CLEANING_SCRIPT.name
            shutil.copy2(CLEANING_SCRIPT, isolated_script)

            env = os.environ.copy()
            env["PYTHONPATH"] = str(temp)
            result = subprocess.run(
                [sys.executable, str(isolated_script)],
                cwd=temp,
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

            output_file = output_dir / "all_survey_collapsed.csv"
            with output_file.open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.DictReader(csv_file))

            sleep_row = next(row for row in rows if row["Survey"] == "Sleep_Survey")
            self.assertEqual(sleep_row["Sleep01"], "3.0")
            self.assertEqual(sleep_row["Sleep02"], "4.0")
            self.assertEqual(sleep_row["Sleep03"], "2.0")


if __name__ == "__main__":
    unittest.main()
