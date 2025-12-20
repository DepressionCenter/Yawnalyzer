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
from sys import stderr
import os
import datetime
from pathlib import Path
import subprocess
import PathKeeper  # File containing machine-specific paths


IDs_list = []
failed_to_render_file = Path(os.path.join(PathKeeper.report_path,"IDs_To_Examine.txt")) #File where IDs that failed to render will be saved

def render_report(
    qmd_file:Path, out_dir: Path, out_html:None, id_param_name:str, id_value:str
):
    cmd = [
        "quarto",
        "render",
        str(qmd_file),
        "-P",
        f"{id_param_name}={id_value}",
        "--to",
        "html",
        "--output-dir",
        str(out_dir),
        "--output",
        str(out_html),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        return (proc.returncode == 0, proc.stdout, proc.stderr)
    except FileNotFoundError:
        return (
            False,
            "",
            "Count not find 'quarto' in PATH. Install Quarto and ensure it is on the PATH",
        )


for file in os.listdir(PathKeeper.merged_data_path):
    if file.endswith("csv"):
        current_file = pd.read_csv(
            Path(os.path.join(PathKeeper.merged_data_path, file))
        )
        current_file = current_file["ID"]
        IDs_list.append(current_file)

id_df = pd.concat(IDs_list, ignore_index=True).sort_values().reset_index(drop=True)


for id in id_df.unique():
    render_datetime = datetime.datetime.now().strftime("%Y_%m_%d_T%H_%M_%S")
    report_html = id + "_QualityContol_" + render_datetime + ".html"
    print(report_html)

    output_path = Path(os.path.join(PathKeeper.merged_data_path, report_html))
    print("Preparing to write", output_path.resolve())

    success, stdout_vale, stderr_value = render_report(
        qmd_file=Path("Yawnalyzer_Step2_PreprocessingReport.qmd"),
        out_html=report_html,
        out_dir=Path(PathKeeper.report_path),
        id_param_name="RID",
        id_value=id,
    )

    if success:
        print("Completed Writing:", report_html)
    else:
        with open(failed_to_render_file, "a", encoding="utf-8") as f:
            f.write(f"Could not Render: {id}")
        print(stderr)

print("Finished Rendering")
