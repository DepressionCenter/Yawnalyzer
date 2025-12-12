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
import pandas as pd
import os
from pathlib import Path
import PathKeeper #File containing machine-specific paths

hr_summary_list = []
sleep_summary_list = []
overall_summary_list = []

summary_path = os.path.join(PathKeeper.summarized_data_path)

for summary_file in os.listdir(summary_path):
    individual_summary = pd.read_csv(os.path.join(summary_path, summary_file))
    if summary_file.startswith("HR"):
        hr_summary_list.append(individual_summary)
    elif summary_file.startswith("Sleep"):
        sleep_summary_list.append(individual_summary)
    elif summary_file.startswith("Overall"):
        overall_summary_list.append(individual_summary)



total_hr = pd.concat(hr_summary_list, ignore_index=True)
total_sleep = pd.concat(sleep_summary_list, ignore_index=True)
total_overall = pd.concat(overall_summary_list, ignore_index=True)


