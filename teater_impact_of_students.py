"""
TEATER Daily Impact Report - Automated Generation Script
Converts Jupyter Notebook queries into production-ready automation

This script:
1. Connects to MySQL database
2. Extracts TEATER metrics (Teach, Engage, Assess, Track, Analyse, Remediate)
3. Generates comprehensive Excel reports
4. Sends email with embedded HTML table and Excel attachment
5. Posts summary to Slack channel

Author: Sridhar Goudu
Organization: Edwisely
"""

import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
import requests
from functools import reduce

# ==========================================
# CONFIGURATION - Load from Environment Variables
# ==========================================
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")


EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Active colleges list
ACTIVE_COLLEGES = (9, 21, 27, 28, 29, 32, 36, 40, 41, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77)

print(f"🔗 Connecting to database: {DB_NAME} at {DB_HOST}")

# ==========================================
# DATABASE CONNECTION
# ==========================================
try:
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={"connect_timeout": 20}
    )
    
    with engine.connect() as connection:
        print("✅ Database connection successful!")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    raise

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def execute_query(query):
    """Execute SQL query and return DataFrame"""
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ Query execution error: {e}")
        raise

# def send_slack_notification(message=None, blocks=None):
#     """Send notification to Slack webhook"""
#     if not SLACK_WEBHOOK_URL:
#         print("⚠️  Slack webhook URL not configured. Skipping Slack notification.")
#         return False
    
#     try:
#         payload = {}
#         if blocks:
#             payload["blocks"] = blocks
#             if message:
#                 payload["text"] = message  # Fallback text
#         else:
#             payload["text"] = message or "📊 Daily TEATER Report Generated"
        
#         response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        
#         if response.status_code == 200:
#             print("✅ Slack notification sent successfully!")
#             return True
#         else:
#             print(f"❌ Slack notification failed: {response.status_code}")
#             return False
#     except Exception as e:
#         print(f"❌ Error sending Slack notification: {e}")
#         return False

# ==========================================
# DATA EXTRACTION - TEACH MODULE
# ==========================================
def get_teach_data():
    """Extract TEACH module metrics - Attendance tracking"""
    print("📚 Fetching TEACH data...")
    
    query = f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(fch.id, '-', sa.student_id)) AS total_attendance
    FROM college c
    LEFT JOIN student_college_details scd 
        ON scd.college_id = c.id
    LEFT JOIN student_attendance sa 
        ON sa.student_id = scd.student_id
    LEFT JOIN faculty_class_hours fch 
        ON sa.faculty_class_hour_id = fch.id
        AND fch.entry_date BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name
    ORDER BY total_attendance DESC;
    """
    
    df = execute_query(query)
    df.fillna(0, inplace=True)
    print(f"   ✓ Found {df['total_attendance'].sum():.0f} attendance records")
    return df

# ==========================================
# DATA EXTRACTION - ENGAGE MODULE
# ==========================================
def get_engage_data():
    """Extract ENGAGE module metrics - All engagement activities"""
    print("💬 Fetching ENGAGE data...")
    
    # Questionnaire Live
    questionnaire_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(ql.id, '-', scd.student_id)) AS total_attendance
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN questionnaire_live_has_students qlhs ON qlhs.student_id = scd.student_id 
    LEFT JOIN questionnaire_live ql ON ql.id = qlhs.questionnaire_id 
        AND ql.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Live Survey
    live_survey_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(ls.id, '-', scd.student_id)) AS total_live_survey
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN live_survey_submissions lss ON lss.student_id = scd.student_id 
    LEFT JOIN live_surveys ls ON ls.id = lss.live_survey_id 
        AND ls.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Notifications
    notify_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(n.id, '-', scd.student_id)) AS total_notify_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN notifications_has_students nhs ON nhs.student_id = scd.student_id
    LEFT JOIN notifications n ON n.id = nhs.notifications_id 
        AND n.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Live Classes
    live_class_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(vc.id, '-', scd.student_id)) AS total_live_class_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN video_conference_has_students vchs ON vchs.student_id = scd.student_id
    LEFT JOIN video_conference vc ON vc.id = vchs.video_conference_id 
        AND vc.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Projects
    project_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(ap.id, '-', scd.student_id)) AS total_projects_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN academic_project_has_students aphs ON aphs.student_id = scd.student_id
    LEFT JOIN academic_projects ap ON ap.id = aphs.academic_project_id 
        AND ap.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Arena (Weekly Challenges)
    arena_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(wc.id, '-', scd.student_id)) AS total_arena_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN weekly_challenge_participants wcp ON wcp.student_id = scd.student_id
    LEFT JOIN weekly_challenge wc ON wc.id = wcp.weekly_challenge_id 
        AND wc.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Go Code
    go_code_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(ap.id, '-', scd.student_id)) AS total_go_code_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN academic_project_has_students aphs ON aphs.student_id = scd.student_id
    LEFT JOIN academic_projects ap ON ap.id = aphs.academic_project_id 
        AND ap.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Merge all engagement metrics
    final_df = (questionnaire_df
        .merge(live_survey_df, on=["college_id", "college_name"], how="left")
        .merge(notify_df, on=["college_id", "college_name"], how="left")
        .merge(live_class_df, on=["college_id", "college_name"], how="left")
        .merge(project_df, on=["college_id", "college_name"], how="left")
        .merge(arena_df, on=["college_id", "college_name"], how="left")
        .merge(go_code_df, on=["college_id", "college_name"], how="left"))
    
    final_df.fillna(0, inplace=True)
    total_engage = final_df.select_dtypes(include='number').sum().sum()
    print(f"   ✓ Found {total_engage:.0f} total engagement activities")
    return final_df

# ==========================================
# DATA EXTRACTION - ASSESS MODULE
# ==========================================
def get_assess_data():
    """Extract ASSESS module metrics - Assessment activities"""
    print("📝 Fetching ASSESS data...")
    
    # Objective Tests
    objective_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(q.id, '-', scd.student_id)) AS total_objective_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN questionnaire_has_students qhs ON qhs.student_id = scd.student_id
    LEFT JOIN questionnaire q ON q.id = qhs.questionnaire_id  
        AND q.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Subjective Tests
    subjective_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(qs.id, '-', scd.student_id)) AS total_subjective_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN questionnaire_subjective_has_students qshs ON qshs.student_id = scd.student_id 
    LEFT JOIN questionnaire_subjective qs ON qs.id = qshs.questionnaire_id
        AND qs.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Coding Tests
    coding_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(ct.id, '-', scd.student_id)) AS total_coding_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN coding_test_has_students cths ON cths.student_id = scd.student_id
    LEFT JOIN coding_test ct ON ct.id = cths.test_id 
        AND ct.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Merge all assessment metrics
    final_df = (objective_df
        .merge(subjective_df, on=["college_id", "college_name"], how="left")
        .merge(coding_df, on=["college_id", "college_name"], how="left"))
    
    final_df.fillna(0, inplace=True)
    total_assess = final_df.select_dtypes(include='number').sum().sum()
    print(f"   ✓ Found {total_assess:.0f} total assessments")
    return final_df

# ==========================================
# DATA EXTRACTION - TRACK MODULE
# ==========================================
def get_track_data():
    """Extract TRACK module metrics - Feedback tracking"""
    print("📊 Fetching TRACK data...")
    
    # Faculty Feedback
    unit_feedback_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(ff.id, '-', scd.student_id)) AS total_faculty_feedback
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN faculty_feedback_students ffs ON ffs.student_id = scd.student_id 
    LEFT JOIN faculty_feedback ff ON ff.id = ffs.feedback_id  
        AND ff.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Semester Feedback
    semester_feedback_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(sf.id, '-', scd.student_id)) AS total_semester_feedback
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN semester_feedback_has_students sfhs ON sfhs.student_id = scd.student_id 
    LEFT JOIN semester_feedback sf ON sf.id = sfhs.semester_feedback_id   
        AND sf.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Regular Feedback
    regular_feedback_df = execute_query(f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(s.id, '-', scd.student_id)) AS total_regular_feedback
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN survey_has_students shs ON shs.student_id = scd.student_id 
    LEFT JOIN survey s ON s.id = shs.survey_id 
        AND s.start_time BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name;
    """)
    
    # Merge all tracking metrics
    final_df = (unit_feedback_df
        .merge(semester_feedback_df, on=["college_id", "college_name"], how="left")
        .merge(regular_feedback_df, on=["college_id", "college_name"], how="left"))
    
    final_df.fillna(0, inplace=True)
    total_track = final_df.select_dtypes(include='number').sum().sum()
    print(f"   ✓ Found {total_track:.0f} total feedback records")
    return final_df

# ==========================================
# DATA EXTRACTION - ANALYSE MODULE
# ==========================================
def get_analyse_data():
    """Extract ANALYSE module metrics - SWOC analysis"""
    print("🔍 Fetching ANALYSE data...")
    
    query = f"""
    SELECT id AS college_id, college_name
    FROM college
    WHERE id IN {ACTIVE_COLLEGES}
    """
    analyse_df = execute_query(query)
    analyse_df["swoc_count"] = 0
    
    print(f"   ✓ Analyse module ready (SWOC: 0)")
    return analyse_df

# ==========================================
# DATA EXTRACTION - REMEDIATE MODULE
# ==========================================
def get_remediate_data():
    """Extract REMEDIATE module metrics - Remediation paths"""
    print("🔧 Fetching REMEDIATE data...")
    
    query = f"""
    SELECT 
        c.id AS college_id,
        c.college_name,
        COUNT(DISTINCT CONCAT(qrp.questionnaire_id, '-', scd.student_id)) AS total_remediate_count
    FROM college c
    LEFT JOIN student_college_details scd ON scd.college_id = c.id
    LEFT JOIN survey_has_students shs ON shs.student_id = scd.student_id 
    LEFT JOIN questionnaire_remedial_path qrp ON qrp.student_id = scd.student_id 
    LEFT JOIN questionnaire q ON q.id = qrp.questionnaire_id  
        AND qrp.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                             AND CONCAT(CURDATE(), ' 08:00:00')
    WHERE c.id IN {ACTIVE_COLLEGES}
    GROUP BY c.id, c.college_name
    ORDER BY total_remediate_count DESC;
    """
    
    df = execute_query(query)
    df.fillna(0, inplace=True)
    total_remediate = df['total_remediate_count'].sum()
    print(f"   ✓ Found {total_remediate:.0f} remediation activities")
    return df

# ==========================================
# GENERATE COMPREHENSIVE REPORTS
# ==========================================
def generate_reports():
    """Generate combined and summary reports"""
    print("\n" + "="*60)
    print("📊 STARTING TEATER REPORT GENERATION")
    print("="*60 + "\n")
    
    # Collect all module data
    teach_df = get_teach_data()
    engage_df = get_engage_data()
    assess_df = get_assess_data()
    track_df = get_track_data()
    analyse_df = get_analyse_data()
    remediate_df = get_remediate_data()
    
    print("\n📦 Merging all datasets...")
    
    # Remove 'name' column if exists
    dfs = [teach_df, engage_df, assess_df, track_df, analyse_df, remediate_df]
    for df in dfs:
        if "name" in df.columns:
            df.drop(columns=["name"], inplace=True)
    
    # Merge all dataframes
    combined_df = reduce(
        lambda left, right: pd.merge(left, right, on=["college_id", "college_name"], how="left"),
        dfs
    )
    combined_df.fillna(0, inplace=True)
    combined_df.insert(0, "S.No", range(1, len(combined_df) + 1))
    
    # ===== CREATE SUMMARY REPORT =====
    print("📋 Creating summary report...")
    
    dfs_dict = {
        "teach": teach_df,
        "engage": engage_df,
        "assess": assess_df,
        "track": track_df,
        "analyse": analyse_df,
        "remediate": remediate_df
    }
    
    result_df = teach_df[["college_id", "college_name"]].copy()
    
    for name, df in dfs_dict.items():
        numeric_cols = [c for c in df.select_dtypes(include="number").columns if c != "college_id"]
        if numeric_cols:
            df[name] = df[numeric_cols].sum(axis=1)
        else:
            df[name] = 0
        result_df = result_df.merge(df[["college_id", name]], on="college_id", how="left")
    
    result_df.fillna(0, inplace=True)
    result_df["total"] = result_df[["teach", "engage", "assess", "track", "analyse", "remediate"]].sum(axis=1)
    result_df = result_df.sort_values(by="total", ascending=False).reset_index(drop=True)
    result_df.insert(0, "S.No", range(1, len(result_df) + 1))
    
    # Convert to integers
    numeric_columns = ["teach", "engage", "assess", "track", "analyse", "remediate", "total"]
    for col in numeric_columns:
        result_df[col] = result_df[col].astype(int)
    
    # Add total row
    total_row = {
        "S.No": "Total",
        "college_id": "-",
        "college_name": "Overall Total"
    }
    for col in numeric_columns:
        total_row[col] = result_df[col].sum()
    
    result_df = pd.concat([result_df, pd.DataFrame([total_row])], ignore_index=True)
    
    print(f"✅ Reports generated successfully!")
    print(f"   • {len(result_df)-1} colleges processed")
    print(f"   • Total activities: {total_row['total']:,}")
    
    return combined_df, result_df

# ==========================================
# SEND EMAIL WITH EXCEL ATTACHMENT
# ==========================================
def send_email_report(result_df, combined_df):
    """Generate Excel and send via email"""
    print("\n📧 Preparing email report...")
    
    output = BytesIO()
    
    # Create Excel file with two sheets
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        result_df.to_excel(writer, sheet_name="Usage_Data", index=False)
        combined_df.to_excel(writer, sheet_name="Individual_Data", index=False)
        
        workbook = writer.book
        worksheet = writer.sheets["Usage_Data"]
        worksheet.autofilter(0, 0, result_df.shape[0], result_df.shape[1] - 1)
        worksheet.freeze_panes(1, 1)
        worksheet.set_column("A:A", 40)
        worksheet.set_column("B:H", 12)
    
    output.seek(0)
    
    # Date formatting
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime('%d-%b-%Y (08:00 AM)')
    end_date = today.strftime('%d-%b-%Y (08:00 AM)')
    
    # Create HTML table
    styled_html = result_df.to_html(index=False, border=0, classes='styled-table')
    
    html_content = f"""
    <html>
    <head>
    <style>
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            background-color: #f7f9fc;
            color: #333;
            margin: 20px;
        }}
        .container {{
            background-color: #ffffff;
            border-radius: 12px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            padding: 25px;
            max-width: 900px;
            margin: auto;
        }}
        h2 {{
            color: #004aad;
            text-align: center;
            margin-bottom: 20px;
        }}
        p {{
            font-size: 15px;
            line-height: 1.6;
        }}
        .styled-table {{
            border-collapse: collapse;
            width: 100%;
            margin-top: 20px;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 0 10px rgba(0,0,0,0.05);
        }}
        .styled-table th {{
            background-color: #007BFF;
            color: white;
            text-align: center;
            padding: 10px;
        }}
        .styled-table td {{
            padding: 8px;
            text-align: center;
            border-bottom: 1px solid #ddd;
        }}
        .styled-table tr:nth-child(even) {{
            background-color: #f3f6fa;
        }}
        .footer {{
            text-align: left;
            font-size: 14px;
            color: #333;
            margin-top: 30px;
            border-top: 1px solid #ddd;
            padding-top: 10px;
        }}
        .signature {{
            margin-top: 20px;
            line-height: 1.5;
            font-size: 14px;
        }}
        .signature b {{
            color: #004aad;
        }}
    </style>
    </head>
    <body>
    <div class="container">
        <h2>📊 Daily Feature Impact Summary Report</h2>
        <p>Hi Sir,</p>
        <p>
            I hope you're doing well.<br><br>
            Please find below the latest <b>Feature impact Table</b> and <b>Chart Summary</b> 
            automatically generated.<br><br>
            This data reflects the usage activity from 
            <b>{start_date}</b> to <b>{end_date}</b>.<br><br>
            Kindly review the insights presented in the table below for your reference.
        </p>
        {styled_html}
        <div class="footer">
            <p><b>Generated on:</b> {today.strftime('%d-%b-%Y %H:%M:%S')}</p>
            <p>✅ This report was automatically generated by the <b>TEATER Analytics System</b>.</p>
        </div>
        <div class="signature">
            <p>
            <b>Warm regards,</b><br>
            Sridhar Goudu<br>
            Engineering Solutions Analyst<br>
            <b>Edwisely</b>
            </p>
        </div>
    </div>
    </body>
    </html>
    """
    
    # Create email message
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📈 Daily TEATER Usage Report - {today.strftime('%d-%b-%Y')}"
    msg["From"] = EMAIL_USER or "sridhar@edwisely.com"
    msg["To"] = "sridhargoudu7@gmail.com"
    msg["Cc"] = "sridhargoudu143@gmail.com"
    
    # Attach HTML body
    msg.attach(MIMEText(html_content, "html"))
    
    # Attach Excel file
    part = MIMEApplication(output.read(), _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    part.add_header('Content-Disposition', 'attachment', filename="IMPACT_TEATER_DAILY_USAGE.xlsx")
    msg.attach(part)

    # ✅ Send via Gmail SMTP
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
        print("✅ Email with embedded table and Excel attachment sent successfully!")
    except Exception as e:
        print("❌ Email send failed:", e)




def teater_generation():
    """Main Lambda handler"""
    try:
        print("🚀 Starting TEATER report generation...")
        
        # Generate reports
        combined_df, result_df = generate_reports()

        # print('combine df ', combined_df, result_df)


        print("generating the output into excel sheets")
        send_email_report(result_df, combined_df)
        print("pivot_table converted successfully")
        

        #sending to mail
        print("✅ Process completed successfully!")
    

        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "statusCode": 500,
            "body": {
                "error": str(e)
            }
        }

# For local testing
if __name__ == "__main__":
    teater_generation()








