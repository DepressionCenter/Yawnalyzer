import PathKeeper
import pandas as pd
import os
import datetime
from pathlib import Path
import subprocess


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
