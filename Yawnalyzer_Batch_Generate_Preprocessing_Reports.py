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
import datetime
from pathlib import Path
import subprocess
import PathKeeper #File containing machine-specific paths


def render_report(
    qmd: Path, out_dir: Path, out_html: None, param_name: str, rid_value: str
):
    cmd = [
        "quarto",
        "render",
        str(qmd),
        "-P",
        f"{param_name}:{rid_value}",
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


hr_collapsed_data = pd.read_csv(
    os.path.join(PathKeeper.merged_data_path, "hr_collapsed.csv")
)

hr_collapsed_data = hr_collapsed_data["ID"].unique()

for id in hr_collapsed_data:
    render_datetime = datetime.datetime.now().strftime("%Y_%m_%d_T%H_%M_%S")
    report_html = id + "_QualityContol_" + render_datetime + ".html"
    print(report_html)
    
    output_path = Path(PathKeeper.report_path) / report_html
    print("Will write:", output_path.resolve())

    render_report(
        qmd="Yawnalyzer_Step2_PreprocessingReport.qmd",
        out_html=report_html,
        out_dir=PathKeeper.report_path,
        param_name="RID",
        rid_value=id,
    )

    print("Completed Writing:", report_html)

print("Finished Rendering")
