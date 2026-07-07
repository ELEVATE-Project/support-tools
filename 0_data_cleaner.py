import pandas as pd
import json
import numpy as np


def clean_participant_data(file_path, output_path):

    df = pd.read_csv(file_path)

    # ---------------------------------------------------
    # Standardize column names for report pipeline
    # Required by 3_final_processor.py
    # ---------------------------------------------------
    df.rename(
        columns={
            "pri_member_info": "PRI Member Information",
            "school_representative_info": "School Representative Information"
        },
        inplace=True
    )

    # If columns are completely missing, create empty ones
    if "PRI Member Information" not in df.columns:
        df["PRI Member Information"] = ""

    if "School Representative Information" not in df.columns:
        df["School Representative Information"] = ""


    def parse_counts(row):

        # default values
        total = 0
        men = 0
        women = 0
        children = 0


        # -----------------------------------------
        # 1. Read existing Men/Women/Children
        # -----------------------------------------
        try:
            men = (
                int(float(row["Men"]))
                if pd.notna(row["Men"])
                else 0
            )

            women = (
                int(float(row["Women"]))
                if pd.notna(row["Women"])
                else 0
            )

            children = (
                int(float(row["Children"]))
                if pd.notna(row["Children"])
                else 0
            )

        except Exception:
            pass


        pc_value = str(
            row.get("Participant Count", "")
        ).strip()


        # -----------------------------------------
        # 2. Participant Count contains JSON
        # Example:
        # {'total':10,'men':2,'women':5}
        # -----------------------------------------
        if pc_value.startswith("{"):

            try:
                json_str = pc_value.replace("'", '"')

                data = json.loads(json_str)


                if data.get("total") not in ["", None]:
                    total = int(data.get("total"))


                if data.get("men") not in ["", None]:
                    men = int(data.get("men"))


                if data.get("women") not in ["", None]:
                    women = int(data.get("women"))


                if data.get("children") not in ["", None]:
                    children = int(data.get("children"))


            except Exception:
                pass


        # -----------------------------------------
        # 3. Participant Count is normal number
        # -----------------------------------------
        elif pc_value.replace(".", "", 1).isdigit():

            total = int(float(pc_value))


        # -----------------------------------------
        # 4. Fix mismatch totals
        # -----------------------------------------
        calculated_total = men + women + children

        if total == 0 or total < calculated_total:
            total = calculated_total


        return pd.Series(
            [
                total,
                men,
                women,
                children
            ]
        )


    # Apply cleaning
    df[
        [
            "Participant Count",
            "Men",
            "Women",
            "Children"
        ]
    ] = df.apply(
        parse_counts,
        axis=1
    )


    # Save output
    df.to_csv(
        output_path,
        index=False
    )


    print(
        f"✅ Data cleaned successfully: {output_path}"
    )

    print(
        "Columns:",
        df.columns.tolist()
    )

    print(
        "Sample participant totals:",
        df["Participant Count"]
        .head()
        .tolist()
    )


if __name__ == "__main__":

    clean_participant_data(
        "KA_chavadi.csv",
        "cleaned_data.csv"
    )