import pandas as pd

def process_csv():
    try:
        # Define the file paths directly in the code
        question_file = "ChandapuraQues.csv"
        status_file = "ChandapuraStatusre.csv"
        output_file = " Chandapura.csv"

        # Load the input CSVs
        question_df = pd.read_csv(question_file)
        status_df = pd.read_csv(status_file)

        # Normalize column names (strip, lowercase, replace spaces with underscores)
        question_df.columns = question_df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_")
        status_df.columns = status_df.columns.astype(str).str.strip().str.lower().str.replace(" ", "_")

        # Required columns in the input
        required_question_columns = ['school_name', 'user_sub_type', 'question', 
                                     'question_response_label', 'evidences']
        required_status_columns = ['school_name', 'submission_date']

        normalized_question_columns = [col.lower().replace(" ", "_") for col in required_question_columns]
        normalized_status_columns = [col.lower().replace(" ", "_") for col in required_status_columns]

        # Check if required columns exist in both files
        if not all(col in question_df.columns for col in normalized_question_columns):
            raise ValueError(f"The question CSV must contain the columns: {', '.join(required_question_columns)}.\n"
                             f"Detected columns: {', '.join(question_df.columns)}")

        if not all(col in status_df.columns for col in normalized_status_columns):
            raise ValueError(f"The status CSV must contain the columns: {', '.join(required_status_columns)}.\n"
                             f"Detected columns: {', '.join(status_df.columns)}")

        # Filter status_df to retain the first non-null submission_date per school_name
        status_df = status_df[status_df['submission_date'].notna()]
        status_df = status_df.sort_values(by=['school_name', 'submission_date'], ascending=[True, False])
        status_df = status_df.drop_duplicates(subset=['school_name'], keep='first')

        # Prepare the data structure for transformation
        transformed_data = []

        # Group by School Name and User Sub Type in question_df
        grouped = question_df.groupby(['school_name', 'user_sub_type'])

        for (school_name, user_type), group in grouped:
            # Initialize a row with blank fields
            row = {'timestamp': '', 'school_name': school_name, 'user_sub_type': user_type}

            # Fetch the submission date from status_df for the current school
            matching_status = status_df[status_df['school_name'] == school_name]
            if not matching_status.empty:
                row['timestamp'] = matching_status.iloc[0]['submission_date']

            # Track the responses and evidences for each question
            question_responses = {}
            question_evidences = {}

            # Process each record in the group
            for _, record in group.iterrows():
                question = record['question']
                response = record['question_response_label']
                evidence = record['evidences'] if pd.notna(record['evidences']) else 'null'

                # Concatenate responses and evidences for the same question
                if question not in question_responses:
                    question_responses[question] = response
                    question_evidences[question] = evidence
                else:
                    question_responses[question] += f", {response}"
                    question_evidences[question] += f", {evidence}"

            # Add each question's responses and evidences to the row
            for question, response in question_responses.items():
                row[question] = response
                row[f"{question}_evidence"] = question_evidences[question]

            # Append the row
            transformed_data.append(row)

        # Create the transformed DataFrame
        transformed_df = pd.DataFrame(transformed_data)

        # Reorder columns: Timestamp, School Name, User Sub Type, Questions..., Evidences
        columns = ['timestamp', 'school_name', 'user_sub_type'] + \
                  [col for col in transformed_df.columns if col not in ['timestamp', 'school_name', 'user_sub_type']] 
        transformed_df = transformed_df[columns]

        # Save the output to a CSV file
        transformed_df.to_csv(output_file, index=False)
        print(f"Transformed CSV has been saved to {output_file}.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    process_csv()