import streamlit as st
import pandas as pd
import json
import re
import os
import shutil
import tempfile
from PIL import Image

# Try importing optional dependencies
try:
    import pytesseract
    import gdown
    from gdown.exceptions import FileURLRetrievalError
except ImportError:
    pytesseract = None
    gdown = None

st.set_page_config(page_title="Data Analysis Tool", layout="wide")
st.title("📄 Report Analysis & Diksha Id extraction Tool")

# Create Tabs
tab1, tab2, tab3 = st.tabs(["📄 Report Analysis", "🔍 DIKSHA ID extraction", "⚖️ Reports comparison"])

# ==========================================
# TAB 1: REPORT ANALYSIS (Original Logic)
# ==========================================
with tab1:
    st.header("Report Analysis")
    
    uploaded_file_report = st.file_uploader(
        "Upload JSON / TXT file (Report)",
        type=["json", "txt"],
        key="uploader_report"
    )

    # -------------------------------
    # Helper: Keep boolean values as True / False
    # -------------------------------
    def normalize_boolean_columns(df):
        for col in df.columns:
            if df[col].dropna().isin([True, False]).all():
                df[col] = df[col].astype(str)
        return df

    # -------------------------------
    # Helper: Value-based multi-column filtering
    # -------------------------------
    def apply_value_filters(df, key_prefix):
        st.markdown("#### 🔍 Filter Rows")
        selected_columns = st.multiselect(
            "Select columns to filter",
            options=df.columns,
            key=f"{key_prefix}_cols"
        )
        filtered_df = df.copy()
        for col in selected_columns:
            value = st.text_input(f"Search value in '{col}'", key=f"{key_prefix}_{col}")
            if value:
                filtered_df = filtered_df[
                    filtered_df[col].astype(str).str.contains(value, case=False, na=False)
                ]
        return filtered_df

    # -------------------------------
    # Helper: Column visibility
    # -------------------------------
    def apply_header_visibility(df, key_prefix):
        st.markdown("#### 🔎 Select Columns to Display")
        keyword = st.text_input("Filter column names by keyword", key=f"{key_prefix}_col_keyword")
        
        if keyword:
            available_columns = [col for col in df.columns if keyword.lower() in col.lower()]
        else:
            available_columns = list(df.columns)
            
        selected_columns = st.multiselect(
            "Choose columns to display",
            options=available_columns,
            default=available_columns[:5] if available_columns else [],
            key=f"{key_prefix}_visible_cols"
        )
        return df[selected_columns] if selected_columns else df

    if uploaded_file_report:
        try:
            data = json.load(uploaded_file_report)
            if "result" not in data or not isinstance(data["result"], list):
                st.error("JSON must contain a 'result' key with list data")
            else:
                result_data = data["result"]
                
                # 1. MAIN RESULT
                main_df = pd.json_normalize(result_data, sep=".")
                main_df = normalize_boolean_columns(main_df)
                main_df.reset_index(drop=True, inplace=True)
                main_df.index += 1
                
                st.subheader("📌 Main Result Data")
                filtered_main = apply_value_filters(main_df, "main")
                final_main = apply_header_visibility(filtered_main, "main")
                st.dataframe(final_main, use_container_width=True)
                
                c1, c2 = st.columns(2)
                c1.download_button("⬇️ DL Full Main", main_df.to_csv(index=False), "main_full.csv")
                c2.download_button("⬇️ DL Filtered Main", final_main.to_csv(index=False), "main_filtered.csv")

                # 2. TASKS
                tasks_rows = []
                for r in result_data:
                    base = {
                        "project_id": r.get("_id"),
                        "userId": r.get("userId"),
                        "programId": r.get("programId"),
                        "solutionId": r.get("solutionId"),
                        "project_title": r.get("title")
                    }
                    for task in r.get("tasks", []):
                        tasks_rows.append({
                            **base,
                            "task_id": task.get("_id"),
                            "task_name": task.get("name"),
                            "task_status": task.get("status"),
                            "task_sequence": task.get("sequenceNumber"),
                            "task_referenceId": task.get("referenceId"),
                            "task_createdAt": task.get("createdAt"),
                            "task_updatedAt": task.get("updatedAt"),
                        })
                
                tasks_df = pd.DataFrame(tasks_rows)
                tasks_df = normalize_boolean_columns(tasks_df)
                
                st.subheader("🧩 Tasks Data")
                if not tasks_df.empty:
                    tasks_df.index += 1
                    filtered_tasks = apply_value_filters(tasks_df, "tasks")
                    final_tasks = apply_header_visibility(filtered_tasks, "tasks")
                    st.dataframe(final_tasks, use_container_width=True)
                    
                    c1, c2 = st.columns(2)
                    c1.download_button("⬇️ DL Full Tasks", tasks_df.to_csv(index=False), "tasks_full.csv")
                    c2.download_button("⬇️ DL Filtered Tasks", final_tasks.to_csv(index=False), "tasks_filtered.csv")
                else:
                    st.info("No tasks found")

                # 3. ATTACHMENTS
                att_rows = []
                for r in result_data:
                    for task in r.get("tasks", []):
                        for att in task.get("attachments", []):
                            att_rows.append({
                                "project_id": r.get("_id"),
                                "task_id": task.get("_id"),
                                "task_name": task.get("name"),
                                "file_name": att.get("name"),
                                "file_type": att.get("type"),
                                "file_url": att.get("url"),
                                "source_path": att.get("sourcePath")
                            })
                
                att_df = pd.DataFrame(att_rows)
                att_df = normalize_boolean_columns(att_df)
                
                st.subheader("📎 Attachments Data")
                if not att_df.empty:
                    att_df.index += 1
                    filtered_att = apply_value_filters(att_df, "attach")
                    final_att = apply_header_visibility(filtered_att, "attach")
                    st.dataframe(final_att, use_container_width=True)
                    
                    c1, c2 = st.columns(2)
                    c1.download_button("⬇️ DL Full Attachments", att_df.to_csv(index=False), "attachments_full.csv")
                    c2.download_button("⬇️ DL Filtered Attachments", final_att.to_csv(index=False), "attachments_filtered.csv")
                else:
                    st.info("No attachments found")

        except Exception as e:
            st.error(f"Failed to process JSON: {e}")

# ==========================================
# TAB 2: DIKSHA ID OCR
# ==========================================
with tab2:
    st.header("DIKSHA ID Extractor")
    
    if pytesseract is None or gdown is None:
        st.error("Missing dependencies! Please wait if installation is in progress or manually install.")
        st.code("pip install pytesseract gdown", language="bash")
        st.warning("Also ensure Tesseract-OCR engine is installed on the system (e.g., `apt-get install tesseract-ocr`).")
        if st.button("Reload App"):
            st.rerun()
    else:
        # --- Helpers ---
        def extract_file_id(drive_link):
            if pd.isna(drive_link): return None
            drive_link = str(drive_link).strip()
            # Robust patterns: stop at / or ? or &
            patterns = [
                r"/file/d/([^/?&]+)", 
                r"/spreadsheets/d/([^/?&]+)",
                r"id=([^&]+)"
            ]
            for p in patterns:
                m = re.search(p, drive_link)
                if m: return m.group(1)
            return None

        def preprocess_image(img):
            img = img.convert("L")
            img = img.resize((img.width * 2, img.height * 2), Image.LANCZOS)
            return img

        def run_ocr(image):
            try:
                processed = preprocess_image(image)
                config = r'-c tessedit_char_whitelist=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_ --psm 6'
                text = pytesseract.image_to_string(processed, config=config)
                lines = [line.strip().lower() for line in text.splitlines() if line.strip()]
                
                for i, line in enumerate(lines):
                    if "diksha" in line:
                        for next_line in lines[i+1:]:
                            if any(x in next_line for x in ["share", "edit", "profile"]): continue
                            if "_" in next_line and len(next_line) >= 8:
                                return next_line
                return None
            except Exception as e:
                # st.error(f"OCR Error: {e}") 
                return None

        # --- UI ---
        ocr_mode = st.radio("Select Mode", ["Single Image Upload", "Batch CSV Processing"])

        if ocr_mode == "Single Image Upload":
            img_file = st.file_uploader("Upload Image", type=["jpg", "png", "jpeg"])
            if img_file:
                image = Image.open(img_file)
                st.image(image, caption="Uploaded Image", width=300)
                if st.button("Extract ID"):
                    with st.spinner("Analyzing..."):
                        res = run_ocr(image)
                        if res:
                            st.code(res, language="text")
                            st.success("**Found DIKSHA ID** (Copy above)")
                        else:
                            st.warning("Could not duplicate DIKSHA ID from this image.")

        elif ocr_mode == "Batch CSV Processing":
            csv_file = st.file_uploader("Upload CSV with Drive Links", type=["csv"])
            if csv_file:
                df = pd.read_csv(csv_file)
                st.write("Preview:", df.head(3))
                
                # Auto-detect column
                default_idx = 0
                for i, col in enumerate(df.columns):
                    if any(x in col.lower() for x in ["link", "url", "drive", "path"]):
                        default_idx = i
                        break
                
                col_name = st.selectbox("Select Column with Drive Links", df.columns, index=default_idx)
                
                if st.button("Start Batch Processing"):
                    results = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Create temp dir for downloads
                    with tempfile.TemporaryDirectory() as temp_dir:
                        total = len(df)
                        for idx, row in df.iterrows():
                            # Update Progress
                            progress_bar.progress((idx + 1) / total)
                            status_text.text(f"Processing row {idx + 1}/{total}...")
                            
                            link = row.get(col_name)
                            f_id = extract_file_id(link)
                            extracted_id = "Not Found"
                            
                            if f_id:
                                save_path = os.path.join(temp_dir, f"{idx}_temp.jpg")
                                url = f"https://drive.google.com/file/d/{f_id}/view"
                                try:
                                    # Download
                                    dl_path = gdown.download(url, save_path, quiet=True, fuzzy=True)
                                    if dl_path and os.path.exists(dl_path):
                                        try:
                                            with Image.open(dl_path) as img:
                                                ocr_res = run_ocr(img)
                                                if ocr_res: extracted_id = ocr_res
                                        except:
                                            extracted_id = "Invalid Image"
                                    else:
                                        extracted_id = "Download Failed"
                                except Exception as e:
                                    extracted_id = "Error"
                            else:
                                extracted_id = "No Valid Link"
                                
                            results.append(extracted_id)
                    
                    df["Processed_Link"] = df[col_name]
                    df["Extracted_DIKSHA_ID"] = results
                    st.success("Processing Complete!")
                    st.dataframe(df)
                    
                    st.download_button(
                        "⬇️ Download Results CSV",
                        df.to_csv(index=False),
                        "ocr_results.csv",
                        "text/csv"
                    )

# ==========================================
# TAB 3: CSV/JSON/TXT MATCHER
# ==========================================
def load_file_to_df(uploaded_file):
    if uploaded_file is None: return None
    
    file_type = uploaded_file.name.split(".")[-1].lower()
    
    try:
        if file_type == "csv":
            return pd.read_csv(uploaded_file)
            
        elif file_type == "json":
            data = json.load(uploaded_file)
            # Handle "Report" format (dict with 'result')
            if isinstance(data, dict) and "result" in data and isinstance(data["result"], list):
                return pd.json_normalize(data["result"], sep=".")
            # Handle list of records
            elif isinstance(data, list):
                return pd.json_normalize(data)
            # Handle single dict
            elif isinstance(data, dict):
                return pd.json_normalize([data])
            else:
                st.error("Unsupported JSON structure")
                return None
                
        elif file_type == "txt":
            # Try parsing as JSON first
            try:
                content = uploaded_file.getvalue().decode("utf-8")
                data = json.loads(content)
                if isinstance(data, (list, dict)):
                    if isinstance(data, dict) and "result" in data:
                        return pd.json_normalize(data["result"], sep=".")
                    return pd.json_normalize(data if isinstance(data, list) else [data])
            except:
                pass
            
            # Fallback: Read as line-separated values
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, header=None, names=["raw_value"])
            return df
            
    except Exception as e:
        st.error(f"Error loading {uploaded_file.name}: {e}")
        return None
    
    return None

with tab3:
    st.header("Data Comparison (CSV/JSON/TXT)")
    st.markdown("Upload files to compare the **Base Data** using values from a **Reference Data**.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Base File (Data to Filter)")
        base_file = st.file_uploader("Upload Base File", type=["csv", "json", "txt"], key="base_uploader")
        
    with col2:
        st.subheader("2. Reference File (Filter List)")
        ref_file = st.file_uploader("Upload Reference File", type=["csv", "json", "txt"], key="ref_uploader")
        
    if base_file and ref_file:
        try:
            df_base = load_file_to_df(base_file)
            df_ref = load_file_to_df(ref_file)
            
            if df_base is not None and df_ref is not None:
                st.write("---")
                c1, c2 = st.columns(2)
                
                with c1:
                    st.info(f"Base Data: {df_base.shape[0]} rows")
                    base_key = st.selectbox("Select Key Column in Base", df_base.columns, key="base_key")
                    
                with c2:
                    st.info(f"Reference Data: {df_ref.shape[0]} rows")
                    ref_key = st.selectbox("Select Key Column in Reference", df_ref.columns, key="ref_key")
                
            if st.button("Find Matches"):
                # Clean keys
                df_base["_match_key"] = df_base[base_key].astype(str).str.strip()
                df_ref["_match_key"] = df_ref[ref_key].astype(str).str.strip()
                
                # Filter
                matched_df = df_base[df_base["_match_key"].isin(df_ref["_match_key"])]
                unmatched_df = df_base[~df_base["_match_key"].isin(df_ref["_match_key"])]
                
                # Cleanup
                matched_df = matched_df.drop(columns=["_match_key"])
                unmatched_df = unmatched_df.drop(columns=["_match_key"])
                
                st.success(f"✅ Found {len(matched_df)} matches!")
                st.progress(len(matched_df) / len(df_base) if len(df_base) > 0 else 0)
                
                st.subheader("Matched Data Preview")
                st.dataframe(matched_df.head(), use_container_width=True)
                
                # Downloads
                d1, d2 = st.columns(2)
                d1.download_button(
                    "⬇️ Download Matched Rows",
                    matched_df.to_csv(index=False),
                    "matched_data.csv"
                )
                if not unmatched_df.empty:
                    d2.download_button(
                        "⬇️ Download Unmatched Rows",
                        unmatched_df.to_csv(index=False),
                        "unmatched_data.csv"
                    )

        except Exception as e:
            st.error(f"Error processing files: {e}")

