import json
import csv

# Load JSON file
json_file = "/home/bharat/reports/OGResponse.json"  # Update with your actual file path
csv_file = "OGResult.csv"  # Output CSV file

with open(json_file, "r", encoding="utf-8") as file:
    data = json.load(file)

result = data.get("result", [])

# Define the fields to extract
fields = ["id", "User Id", "User SubType", "Declared State", "Declared District", "Declared Block",
          "Declared School ID", "Declared School Name", "Organisation Name", "Program Name", "Program ID",
          "Observation Name", "Observation ID", "District observed", "Block observed", "School observed",
          "observation_criteria_id", "Submission date", "Status"]

# Open CSV file for writing
with open(csv_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(fields)

    for record in result:
        base_row = [
            record.get("_id", ""),
            record.get("createdBy", ""),
            ", ".join([role.get("label", "") for role in record.get("userProfile", {}).get("user_roles", []) if isinstance(role, dict)]),
            record.get("userProfile", {}).get("state", {}).get("label", ""),
            record.get("userProfile", {}).get("district", {}).get("label", ""),
            record.get("userProfile", {}).get("block", {}).get("label", ""),
            record.get("userProfile", {}).get("school", {}).get("externalId", ""),
            record.get("userProfile", {}).get("school", {}).get("label", ""),
            record.get("userProfile", {}).get("organization", {}).get("name", ""),
            record.get("program_name", ""),
            record.get("programExternalId", ""),
            record.get("observationInformation", {}).get("name", ""),
            record.get("observationInformation", {}).get("solutionExternalId", ""),
            record.get("observationInformation", {}).get("userProfile", {}).get("district", {}).get("label", ""),
            record.get("observationInformation", {}).get("userProfile", {}).get("block", {}).get("label", ""),
            record.get("observationInformation", {}).get("userProfile", {}).get("school", {}).get("label", ""),
        ]

        evidences_status = record.get("evidencesStatus", [])
        row_written = False

        if isinstance(evidences_status, list) and len(evidences_status) > 0:
            for evidence in evidences_status:
                submissions = evidence.get("submissions", [])
                if isinstance(submissions, list) and len(submissions) > 0:
                    for submission in submissions:
                        submission_id = submission.get("externalId", "")
                        submission_date = submission.get("submissionDate", "")

                        row = base_row.copy()
                        row.append(submission_id)
                        row.append(submission_date)
                        row.append(record.get("status", ""))
                        writer.writerow(row)
                        row_written = True

        if not row_written:
            row = base_row.copy()
            row.append("")  # Empty submission ID
            row.append("")  # Empty submission date
            row.append(record.get("status", ""))
            writer.writerow(row)

print(f"CSV file saved as {csv_file}")
