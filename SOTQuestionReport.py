import json
import csv


json_file = "/home/bharat/reports/OGResponse.json"  
csv_file = "OGQuestionReport.csv"  

with open(json_file, "r", encoding="utf-8") as file:
    data = json.load(file) 

result = data.get("result", [])

fields = ["id", "observation Id", "Status", "User Id", "User SubType", "Declared State", "Declared District", "Declared Block",
          "Declared School ID", "Declared School Name", "Organisation Name", "Program Name", "Program ID",
          "Observation Name", "Observation ID", "District observed", "Block observed", "School observed",
          "observation_criteria_id", "Submission date", "Evidence URL",
          "User name", "Title", "Question", "Answer", "Max Score", "Percentage Score","points Based Score", "Score Achieved", "Weightage"]


with open(csv_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    
    
    writer.writerow(fields)

    
    for record in result:

        base_row = [
            record.get("_id", ""),
            record.get("observationId", ""),
            record.get("status", ""),
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
            record.get("userProfile", {}).get("name", ""),
            record.get("title", ""),
        ]

        # Extract answers dictionary
        answers_dict = record.get("answers", {})

        # Iterate through `evidencesStatus` to generate multiple rows
        evidences_status = record.get("evidencesStatus", [])

        if isinstance(evidences_status, list) and len(evidences_status) > 0:
            for evidence in evidences_status:
                submissions = evidence.get("submissions", [])
                if isinstance(submissions, list) and len(submissions) > 0:
                    for submission in submissions:
                        submission_date = submission.get("submissionDate", "")
                        submission_id = submission.get("externalId", "")  # Fetch submissionId

                        # Find matching answers
                        matched_answers = [
                            ans for ans in answers_dict.values() 
                            if ans.get("evidenceMethod") == submission_id
                        ]
                        url = ""
                        if matched_answers:
                            for ans in matched_answers:
                                URls = ans.get("fileName", [])
                                if isinstance(URls, list) and len(URls) > 0:
                                    for URl in URls:
                                        url = URl.get("url", "")
                                        # submission_id = submission.get("externalId", "")  # Fetch submissionId
                                payload = ans.get("payload", {})
                                question = payload.get("question", "N/A")
                                answer = payload.get("labels", "N/A")
                                max_score = ans.get("maxScore", "")
                                percentage_score = ans.get("percentageScore", "")
                                pointsBasedScore = ans.get("pointsBasedScoreInParent","")
                                score_achieved = ans.get("scoreAchieved", "")
                                weightage = ans.get("weightage", "")


                                # Create a new row per question-answer pair
                                row = base_row.copy()
                                row.insert(18, submission_id)  # Insert submission_id at correct position
                                row.insert(19, submission_date)  # Insert submission_date at correct position
                                row.insert(20, url)
                                row.append(question)  # Append question
                                row.append(answer)  # Append answer
                                row.append(max_score)  # Append max score
                                row.append(percentage_score)  # Append percentage score
                                row.append(pointsBasedScore)
                                row.append(score_achieved)  # Append score achieved
                                row.append(weightage)  # Append weightage
                                writer.writerow(row)
                        else:
                            # If no matching answers, write a row with "No Question" and "No Answer"
                            row = base_row.copy()
                            row.insert(18, submission_id)
                            row.insert(19, submission_date)
                            row.insert(20, "NA")
                            row.append("No Question")
                            row.append("No Answer")
                            row.append("")  # Empty max score
                            row.append("")  # Empty percentage score
                            row.append("")
                            row.append("")  # Empty score achieved
                            row.append("")  # Empty weightage
                            writer.writerow(row)
        else:
            # If no evidencesStatus, write a single row with empty submission fields
            row = base_row.copy()
            row.insert(18, "") 
            row.insert(19, "")  
            row.insert(20,"NA")
            row.append("No Question")
            row.append("No Answer")
            row.append("") 
            row.append("") 
            row.append("")
            row.append("") 
            row.append("") 
            writer.writerow(row)

print(f"CSV file saved as {csv_file}")
