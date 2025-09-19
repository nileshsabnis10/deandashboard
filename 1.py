# Dean Dashboard — UI v4.1.5 (Read-Only, auto-detects marks sheets for download)
# Built: 19 Sep 2025, 10:55 AM IST
# Notes:
#   • No longer requires 'DataTabName' in _Config. The app now automatically finds matching sheet names.
#   • This relies on a consistent naming convention for mark sheets (e.g., sheet for component 'T1' is named 'T1' or 'T1_Marks').
#   • Re-enabled inline download buttons for individual locked component marks.

import os, json, re, io, time
from typing import Dict, List, Tuple
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

# ==============================
# Constants / Meta
# ==============================
DASHBOARD_VERSION = "4.1.5"
LAST_BUILD_STR = "19 Sep 2025, 10:55 AM IST"

# ==============================
# Read-only configuration
# ==============================
SCOPES = ["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/spreadsheets.readonly"]

def _secrets():
    try: return dict(st.secrets)
    except Exception: return {}

def load_settings() -> Dict:
    s = _secrets()
    drive = s.get("drive", {}); paths = s.get("paths", {})
    parent_folder_id = drive.get("parent_folder_id") or os.getenv("PARENT_FOLDER_ID","").strip()
    if not parent_folder_id: st.error("Missing drive.parent_folder_id in secrets.", icon="🚫"); st.stop()
    standard_classes = drive.get("standard_classes") or ["First Year","Second Year","Third Year","Final Year"]
    read_sa = paths.get("read_service_account_file") or os.getenv("READ_SA_FILE","service-account.json")
    return {"PARENT_FOLDER_ID": parent_folder_id, "STANDARD_CLASSES": list(standard_classes), "READ_SA_FILE": os.path.expanduser(read_sa)}

SET = load_settings()
PARENT_FOLDER_ID = SET["PARENT_FOLDER_ID"]
STANDARD_CLASSES = SET["STANDARD_CLASSES"]

# ==============================
# Auth (READ ONLY)
# ==============================
def _creds_from_path_or_json(path_or_json: str):
    if os.path.exists(path_or_json): return Credentials.from_service_account_file(path_or_json, scopes=SCOPES)
    try: data = json.loads(path_or_json); return Credentials.from_service_account_info(data, scopes=SCOPES)
    except Exception: raise RuntimeError("READ SA not found.")

CREDS = _creds_from_path_or_json(SET["READ_SA_FILE"])
SESSION = AuthorizedSession(CREDS)

# ==============================
# API Helpers
# ==============================
def drive_list(q: str, fields: str = "files(id,name,mimeType,parents)") -> List[Dict]:
    url = "https://www.googleapis.com/drive/v3/files"
    params = {"q": q, "fields": fields, "pageSize": 1000, "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"}
    r = SESSION.get(url, params=params, timeout=60); r.raise_for_status()
    return r.json().get("files", [])

def list_child_folders(parent_id: str) -> List[Dict]:
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    return sorted(drive_list(q, "files(id,name)"), key=lambda f: f["name"].lower())

def list_class_spreadsheets(class_folder_id: str) -> List[Dict]:
    q = f"'{class_folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    return drive_list(q, "files(id,name)")

def _df_from_values(values: List[List[str]]) -> pd.DataFrame:
    if not values: return pd.DataFrame()
    cols = values[0] if values else []; rows = values[1:] if len(values) > 1 else []
    try: df = pd.DataFrame(rows, columns=cols)
    except Exception:
        df = pd.DataFrame(rows)
        if cols: df.columns = cols[:df.shape[1]]
    return df

@st.cache_data(ttl=600, show_spinner=False)
def load_tab(ssid: str, title: str) -> pd.DataFrame:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/values:batchGet"
    params = {"ranges": [f"'{title}'!A1:ZZZ"], "majorDimension": "ROWS"}
    r = SESSION.get(url, params=params, timeout=60)
    if r.status_code in (400,404): return pd.DataFrame()
    r.raise_for_status()
    vr = (r.json().get("valueRanges") or [{}])[0]
    return _df_from_values(vr.get("values") or [])

@st.cache_data(ttl=600, show_spinner=False)
def get_sheet_id_map(ssid: str) -> Dict[str,int]:
    r = SESSION.get(f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}", params={"fields": "sheets(properties(sheetId,title))"}, timeout=60)
    r.raise_for_status()
    return {p.get("properties",{}).get("title"): p.get("properties",{}).get("sheetId") for p in r.json().get("sheets", [])}

# ==============================
# Domain Logic
# ==============================
def is_class_final_approved(ssid: str, klass: str) -> bool:
    ap = load_tab(ssid, "_Approvals")
    if ap.empty: return False
    rows = ap.iloc[1:] if list(ap.columns)[0] == "Scope" else ap
    mask = (rows.iloc[:,0].astype(str).str.strip()=="ClassFinal") & (rows.iloc[:,1].astype(str).str.strip()==klass)
    return bool(mask.any())

def _prep_cfg(cfg: pd.DataFrame) -> pd.DataFrame:
    if cfg.empty: return pd.DataFrame(columns=["Class","Course","CourseCode","Component","_class_lower","_code_lower","_comp_lower"])
    c = cfg.copy()
    for col in ["Class", "CourseCode", "Component"]:
        if col not in c.columns: c[col] = ""
    c["_class_lower"] = c["Class"].astype(str).str.strip().str.lower()
    c["_code_lower"]  = c["CourseCode"].astype(str).str.strip().str.lower()
    c["_comp_lower"]  = c["Component"].astype(str).str.strip().str.lower()
    return c

def all_components_locked_for_class(cfg: pd.DataFrame, audit: pd.DataFrame, klass: str) -> Tuple[int,int,bool]:
    if cfg.empty: return 0,0,False
    c = cfg[cfg["_class_lower"] == str(klass).lower()].copy()
    total = len(c)
    if total == 0 or audit.empty: return 0,total,False
    au = audit.copy()
    for col in ["Course","Component","Action"]:
        if col not in au.columns: au[col] = ""
    au["key"] = au["Course"].str.lower().str.strip() + "||" + au["Component"].str.lower().str.strip()
    c["key"] = c["_code_lower"] + "||" + c["_comp_lower"]
    locked = c["key"].isin(set(au[au["Action"].str.lower()=="locked"]["key"])).sum()
    return int(locked), int(total), bool(locked==total)

def get_class_cutoff_display(ssid: str) -> str:
    appset = load_tab(ssid, "_AppSettings")
    if appset.empty or "Key" not in appset.columns or "Value" not in appset.columns: return ""
    v = appset.loc[appset["Key"]=="LockCutoffISO", "Value"]
    if v.empty: return ""
    raw = v.iloc[0].strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z","+00:00")); ist = timezone(timedelta(hours=5, minutes=30))
        dt = dt.astimezone(ist) if dt.tzinfo else dt.replace(tzinfo=ist)
        return dt.strftime("%d/%m/%Y %H:%M IST")
    except Exception: return raw

def per_course_lock_table(cfg: pd.DataFrame, audit: pd.DataFrame, klass: str) -> pd.DataFrame:
    if cfg.empty: return pd.DataFrame()
    c = cfg[cfg["_class_lower"] == str(klass).lower()].copy()
    if c.empty: return pd.DataFrame()
    a = audit.copy()
    for col in ["Course","Component","Action"]:
        if col not in a.columns: a[col] = ""
    a["Locked"] = a["Action"].astype(str).str.lower().eq("locked")
    a["key"] = a["Course"].str.lower().str.strip() + "||" + a["Component"].str.lower().str.strip()
    c["key"] = c["_code_lower"] + "||" + c["_comp_lower"]
    c["IsLocked"] = c["key"].isin(set(a[a["Locked"]]["key"]))
    view = c[["CourseCode","Course","Component","IsLocked"]].drop_duplicates().sort_values(by=["CourseCode","Component"])
    view["Status"] = view["IsLocked"].map({True:"🔒 Locked", False:"🔓 Unlocked"})
    return view[["CourseCode","Course","Component","Status"]]

# --- NEW HELPER FUNCTION to find sheet names automatically ---
def find_matching_marks_tab(component_name: str, all_titles: List[str]) -> str:
    comp_norm = component_name.lower().strip()
    titles_norm = {title: title.lower().strip() for title in all_titles}
    
    # Define search patterns in order of priority
    patterns = [
        f"{comp_norm}",
        f"{comp_norm}_marks",
        f"{comp_norm} marks",
        f"marks_{comp_norm}",
        f"marks {comp_norm}",
    ]
    
    # First, check for common, specific patterns
    for title, norm_title in titles_norm.items():
        if norm_title in patterns:
            return title
            
    # If no specific pattern found, try a looser search
    for title, norm_title in titles_norm.items():
        if comp_norm in norm_title:
            return title

    return "" # No match found

def _norm(s: str) -> str: return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip()

# ... (rest of the domain logic and helpers are unchanged)
def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_csv(index=False).encode("utf-8-sig")
def _slug(*parts: str) -> str:
    s = " ".join(map(str, parts)); s = re.sub(r"[^\w\-]+", "_", s); s = re.sub(r"_+", "_", s).strip("_"); return s or "export"

# ==============================
# UI
# ==============================
st.set_page_config(page_title="Dean's Dashboard — Read Only", layout="wide", page_icon="📊")
st.markdown("""<style>.topbar{height:6px;background:linear-gradient(90deg,#e11d48,#f59e0b,#22c55e,#3b82f6);border-radius:8px;margin-bottom:10px;}.kpi{border-radius:14px;padding:16px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 2px 6px rgba(0,0,0,.04);background:#fff;}</style><div class="topbar"></div>""", unsafe_allow_html=True)

title_col, meta_col = st.columns([3,1])
with title_col: st.title("📊 Progress Dashboard"); st.caption("Read-only visibility into internal marks submission and approvals.")
with meta_col: st.metric("Version", DASHBOARD_VERSION); st.caption(f"Last Build: {LAST_BUILD_STR}")

nav = st.radio("View", ["Overview", "Class View"], horizontal=True)

dept_folders = list_child_folders(PARENT_FOLDER_ID)
dept_names = [f["name"] for f in dept_folders]
if not dept_names: st.error("No department folders found under the parent.", icon="🚫"); st.stop()
dept_id_map = {f["name"]: f["id"] for f in dept_folders}

if nav == "Overview":
    colA, colB = st.columns([2,2]);
    with colA: dept = st.selectbox("Program / Department", sorted(dept_names), index=0)
    with colB: selected_classes = st.multiselect("Classes", STANDARD_CLASSES, default=STANDARD_CLASSES)
    if not selected_classes: st.info("Select at least one class to summarize.")
    else:
        rows = []
        for k in selected_classes:
            class_folders = list_child_folders(dept_id_map[dept]); kf = next((f for f in class_folders if f["name"].strip().lower()==k.strip().lower()), None)
            if not kf: rows.append({"Class": k, "Workbook": "—", "Locked/Total": "0/0", "% Complete": 0.0, "Final Approval":"—"}); continue
            candidates = list_class_spreadsheets(kf["id"])
            if not candidates: rows.append({"Class": k, "Workbook": "—", "Locked/Total": "0/0", "% Complete": 0.0, "Final Approval":"—"}); continue
            prio = [x for x in candidates if x["name"].lower().endswith("_marks")]; meta = prio[0] if prio else candidates[0]; ssid, ssname = meta["id"], meta["name"]
            cfg = _prep_cfg(load_tab(ssid, "_Config")); audit = load_tab(ssid, "_Audit"); locked, total, _ = all_components_locked_for_class(cfg, audit, k)
            pct = round((locked/total*100.0),1) if total else 0.0; approval = "✅ Yes" if is_class_final_approved(ssid, k) else "⏳ No"
            rows.append({"Class": k, "Workbook": ssname, "Locked/Total": f"{locked}/{total}", "% Complete": pct, "Final Approval": approval})
        df = pd.DataFrame(rows); st.subheader(f"Overview — {dept}"); st.markdown('<div class="kpi">', unsafe_allow_html=True); c1,c2,c3 = st.columns(3)
        avg = float(df["% Complete"].mean()) if not df.empty else 0.0; c1.metric("Average Completion", f"{avg:.1f}%"); c2.metric("Approved", f"{int((df['Final Approval']=='✅ Yes').sum())} / {len(df)}"); c3.metric("Classes", f"{len(df)}")
        st.markdown('</div>', unsafe_allow_html=True); st.markdown(""); st.dataframe(df, use_container_width=True, height=320)
        try: st.bar_chart(df[["Class","% Complete"]].set_index("Class"))
        except Exception: st.caption("Chart unavailable for current data.")

elif nav == "Class View":
    colA, colB = st.columns([2,2])
    with colA: dept = st.selectbox("Program / Department", sorted(dept_names), index=0, key="cls_dept")
    with colB: klass = st.selectbox("Class", STANDARD_CLASSES, index=0, key="cls_class")

    ssid, ssname = "", ""
    class_folders = list_child_folders(dept_id_map[dept]); kf = next((f for f in class_folders if f["name"].strip().lower()==klass.strip().lower()), None)
    if not kf: st.info("Selected class folder not found under this department.")
    else:
        candidates = list_class_spreadsheets(kf["id"])
        if not candidates: st.info("No spreadsheet in this class folder.")
        else: prio = [x for x in candidates if x["name"].lower().endswith("_marks")]; meta = prio[0] if prio else candidates[0]; ssid, ssname = meta["id"], meta["name"]

    if ssid:
        st.markdown(f"**Workbook:** `{ssname}`")
        cfg = _prep_cfg(load_tab(ssid, "_Config")); audit = load_tab(ssid, "_Audit"); locked, total, _ = all_components_locked_for_class(cfg, audit, klass)
        c1,c2,c3 = st.columns(3); c1.metric("Components Locked", f"{locked} / {total}"); c2.metric("Final Approval", "✅ Approved" if is_class_final_approved(ssid, klass) else "⏳ Pending"); c3.metric("Lock Cutoff", get_class_cutoff_display(ssid) or "—")
        st.progress(min(100, int((locked/total*100.0) if total else 0)))

        st.subheader("Per-Course Component Status")
        # Get all sheet titles once for efficiency
        all_sheet_titles = list(get_sheet_id_map(ssid).keys())
        tbl = per_course_lock_table(cfg, audit, klass)
        
        if tbl.empty: st.info("No _Config found for this class.")
        else:
            header_cols = st.columns([2, 4, 2, 2])
            header_cols[0].markdown("**Course Code**")
            header_cols[1].markdown("**Course**")
            header_cols[2].markdown("**Component**")
            header_cols[3].markdown("**Status / Action**")
            st.markdown("---")

            for _, row in tbl.iterrows():
                row_cols = st.columns([2, 4, 2, 2])
                row_cols[0].write(row["CourseCode"])
                row_cols[1].write(row["Course"])
                row_cols[2].write(row["Component"])
                
                with row_cols[3]:
                    if row["Status"] == "🔒 Locked":
                        component_name = row["Component"]
                        # --- MODIFIED LOGIC: Find tab name automatically ---
                        marks_tab_name = find_matching_marks_tab(component_name, all_sheet_titles)
                        
                        if marks_tab_name:
                            marks_df = load_tab(ssid, marks_tab_name)
                            if not marks_df.empty:
                                csv_bytes = df_to_csv_bytes(marks_df)
                                file_name = _slug(dept, klass, row['CourseCode'], row['Component'], "Marks") + ".csv"
                                st.download_button(
                                    label="⬇️ Download Marks",
                                    data=csv_bytes,
                                    file_name=file_name,
                                    key=f"dl_{ssid}_{row['CourseCode']}_{row['Component']}"
                                )
                            else:
                                st.caption("Empty Sheet")
                        else:
                            st.caption("Sheet Not Found")
                    else:
                        st.write("🔓 Unlocked")

        # ... (Final/Provisional downloads code is unchanged) ...
        st.subheader("Class Final (Approved / Provisional)")
        st.caption("Final result sheets are loaded from tabs containing the word 'Final'.")

# Footer
st.markdown("---")
st.caption("© SGU Internal Marks Management System — Nilesh Vijay Sabnis")
st.caption(f"Dashboard Version {DASHBOARD_VERSION} | Last Build: {LAST_BUILD_STR}")# Dean Dashboard — UI v4.1.5 (Read-Only, auto-detects marks sheets for download)
# Built: 19 Sep 2025, 10:55 AM IST
# Notes:
#   • No longer requires 'DataTabName' in _Config. The app now automatically finds matching sheet names.
#   • This relies on a consistent naming convention for mark sheets (e.g., sheet for component 'T1' is named 'T1' or 'T1_Marks').
#   • Re-enabled inline download buttons for individual locked component marks.

import os, json, re, io, time
from typing import Dict, List, Tuple
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

# ==============================
# Constants / Meta
# ==============================
DASHBOARD_VERSION = "4.1.5"
LAST_BUILD_STR = "19 Sep 2025, 10:55 AM IST"

# ==============================
# Read-only configuration
# ==============================
SCOPES = ["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/spreadsheets.readonly"]

def _secrets():
    try: return dict(st.secrets)
    except Exception: return {}

def load_settings() -> Dict:
    s = _secrets()
    drive = s.get("drive", {}); paths = s.get("paths", {})
    parent_folder_id = drive.get("parent_folder_id") or os.getenv("PARENT_FOLDER_ID","").strip()
    if not parent_folder_id: st.error("Missing drive.parent_folder_id in secrets.", icon="🚫"); st.stop()
    standard_classes = drive.get("standard_classes") or ["First Year","Second Year","Third Year","Final Year"]
    read_sa = paths.get("read_service_account_file") or os.getenv("READ_SA_FILE","service-account.json")
    return {"PARENT_FOLDER_ID": parent_folder_id, "STANDARD_CLASSES": list(standard_classes), "READ_SA_FILE": os.path.expanduser(read_sa)}

SET = load_settings()
PARENT_FOLDER_ID = SET["PARENT_FOLDER_ID"]
STANDARD_CLASSES = SET["STANDARD_CLASSES"]

# ==============================
# Auth (READ ONLY)
# ==============================
def _creds_from_path_or_json(path_or_json: str):
    if os.path.exists(path_or_json): return Credentials.from_service_account_file(path_or_json, scopes=SCOPES)
    try: data = json.loads(path_or_json); return Credentials.from_service_account_info(data, scopes=SCOPES)
    except Exception: raise RuntimeError("READ SA not found.")

CREDS = _creds_from_path_or_json(SET["READ_SA_FILE"])
SESSION = AuthorizedSession(CREDS)

# ==============================
# API Helpers
# ==============================
def drive_list(q: str, fields: str = "files(id,name,mimeType,parents)") -> List[Dict]:
    url = "https://www.googleapis.com/drive/v3/files"
    params = {"q": q, "fields": fields, "pageSize": 1000, "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"}
    r = SESSION.get(url, params=params, timeout=60); r.raise_for_status()
    return r.json().get("files", [])

def list_child_folders(parent_id: str) -> List[Dict]:
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    return sorted(drive_list(q, "files(id,name)"), key=lambda f: f["name"].lower())

def list_class_spreadsheets(class_folder_id: str) -> List[Dict]:
    q = f"'{class_folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    return drive_list(q, "files(id,name)")

def _df_from_values(values: List[List[str]]) -> pd.DataFrame:
    if not values: return pd.DataFrame()
    cols = values[0] if values else []; rows = values[1:] if len(values) > 1 else []
    try: df = pd.DataFrame(rows, columns=cols)
    except Exception:
        df = pd.DataFrame(rows)
        if cols: df.columns = cols[:df.shape[1]]
    return df

@st.cache_data(ttl=600, show_spinner=False)
def load_tab(ssid: str, title: str) -> pd.DataFrame:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/values:batchGet"
    params = {"ranges": [f"'{title}'!A1:ZZZ"], "majorDimension": "ROWS"}
    r = SESSION.get(url, params=params, timeout=60)
    if r.status_code in (400,404): return pd.DataFrame()
    r.raise_for_status()
    vr = (r.json().get("valueRanges") or [{}])[0]
    return _df_from_values(vr.get("values") or [])

@st.cache_data(ttl=600, show_spinner=False)
def get_sheet_id_map(ssid: str) -> Dict[str,int]:
    r = SESSION.get(f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}", params={"fields": "sheets(properties(sheetId,title))"}, timeout=60)
    r.raise_for_status()
    return {p.get("properties",{}).get("title"): p.get("properties",{}).get("sheetId") for p in r.json().get("sheets", [])}

# ==============================
# Domain Logic
# ==============================
def is_class_final_approved(ssid: str, klass: str) -> bool:
    ap = load_tab(ssid, "_Approvals")
    if ap.empty: return False
    rows = ap.iloc[1:] if list(ap.columns)[0] == "Scope" else ap
    mask = (rows.iloc[:,0].astype(str).str.strip()=="ClassFinal") & (rows.iloc[:,1].astype(str).str.strip()==klass)
    return bool(mask.any())

def _prep_cfg(cfg: pd.DataFrame) -> pd.DataFrame:
    if cfg.empty: return pd.DataFrame(columns=["Class","Course","CourseCode","Component","_class_lower","_code_lower","_comp_lower"])
    c = cfg.copy()
    for col in ["Class", "CourseCode", "Component"]:
        if col not in c.columns: c[col] = ""
    c["_class_lower"] = c["Class"].astype(str).str.strip().str.lower()
    c["_code_lower"]  = c["CourseCode"].astype(str).str.strip().str.lower()
    c["_comp_lower"]  = c["Component"].astype(str).str.strip().str.lower()
    return c

def all_components_locked_for_class(cfg: pd.DataFrame, audit: pd.DataFrame, klass: str) -> Tuple[int,int,bool]:
    if cfg.empty: return 0,0,False
    c = cfg[cfg["_class_lower"] == str(klass).lower()].copy()
    total = len(c)
    if total == 0 or audit.empty: return 0,total,False
    au = audit.copy()
    for col in ["Course","Component","Action"]:
        if col not in au.columns: au[col] = ""
    au["key"] = au["Course"].str.lower().str.strip() + "||" + au["Component"].str.lower().str.strip()
    c["key"] = c["_code_lower"] + "||" + c["_comp_lower"]
    locked = c["key"].isin(set(au[au["Action"].str.lower()=="locked"]["key"])).sum()
    return int(locked), int(total), bool(locked==total)

def get_class_cutoff_display(ssid: str) -> str:
    appset = load_tab(ssid, "_AppSettings")
    if appset.empty or "Key" not in appset.columns or "Value" not in appset.columns: return ""
    v = appset.loc[appset["Key"]=="LockCutoffISO", "Value"]
    if v.empty: return ""
    raw = v.iloc[0].strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z","+00:00")); ist = timezone(timedelta(hours=5, minutes=30))
        dt = dt.astimezone(ist) if dt.tzinfo else dt.replace(tzinfo=ist)
        return dt.strftime("%d/%m/%Y %H:%M IST")
    except Exception: return raw

def per_course_lock_table(cfg: pd.DataFrame, audit: pd.DataFrame, klass: str) -> pd.DataFrame:
    if cfg.empty: return pd.DataFrame()
    c = cfg[cfg["_class_lower"] == str(klass).lower()].copy()
    if c.empty: return pd.DataFrame()
    a = audit.copy()
    for col in ["Course","Component","Action"]:
        if col not in a.columns: a[col] = ""
    a["Locked"] = a["Action"].astype(str).str.lower().eq("locked")
    a["key"] = a["Course"].str.lower().str.strip() + "||" + a["Component"].str.lower().str.strip()
    c["key"] = c["_code_lower"] + "||" + c["_comp_lower"]
    c["IsLocked"] = c["key"].isin(set(a[a["Locked"]]["key"]))
    view = c[["CourseCode","Course","Component","IsLocked"]].drop_duplicates().sort_values(by=["CourseCode","Component"])
    view["Status"] = view["IsLocked"].map({True:"🔒 Locked", False:"🔓 Unlocked"})
    return view[["CourseCode","Course","Component","Status"]]

# --- NEW HELPER FUNCTION to find sheet names automatically ---
def find_matching_marks_tab(component_name: str, all_titles: List[str]) -> str:
    comp_norm = component_name.lower().strip()
    titles_norm = {title: title.lower().strip() for title in all_titles}
    
    # Define search patterns in order of priority
    patterns = [
        f"{comp_norm}",
        f"{comp_norm}_marks",
        f"{comp_norm} marks",
        f"marks_{comp_norm}",
        f"marks {comp_norm}",
    ]
    
    # First, check for common, specific patterns
    for title, norm_title in titles_norm.items():
        if norm_title in patterns:
            return title
            
    # If no specific pattern found, try a looser search
    for title, norm_title in titles_norm.items():
        if comp_norm in norm_title:
            return title

    return "" # No match found

def _norm(s: str) -> str: return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip()

# ... (rest of the domain logic and helpers are unchanged)
def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_csv(index=False).encode("utf-8-sig")
def _slug(*parts: str) -> str:
    s = " ".join(map(str, parts)); s = re.sub(r"[^\w\-]+", "_", s); s = re.sub(r"_+", "_", s).strip("_"); return s or "export"

# ==============================
# UI
# ==============================
st.set_page_config(page_title="Dean's Dashboard — Read Only", layout="wide", page_icon="📊")
st.markdown("""<style>.topbar{height:6px;background:linear-gradient(90deg,#e11d48,#f59e0b,#22c55e,#3b82f6);border-radius:8px;margin-bottom:10px;}.kpi{border-radius:14px;padding:16px;border:1px solid rgba(0,0,0,0.06);box-shadow:0 2px 6px rgba(0,0,0,.04);background:#fff;}</style><div class="topbar"></div>""", unsafe_allow_html=True)

title_col, meta_col = st.columns([3,1])
with title_col: st.title("📊 Progress Dashboard"); st.caption("Read-only visibility into internal marks submission and approvals.")
with meta_col: st.metric("Version", DASHBOARD_VERSION); st.caption(f"Last Build: {LAST_BUILD_STR}")

nav = st.radio("View", ["Overview", "Class View"], horizontal=True)

dept_folders = list_child_folders(PARENT_FOLDER_ID)
dept_names = [f["name"] for f in dept_folders]
if not dept_names: st.error("No department folders found under the parent.", icon="🚫"); st.stop()
dept_id_map = {f["name"]: f["id"] for f in dept_folders}

if nav == "Overview":
    colA, colB = st.columns([2,2]);
    with colA: dept = st.selectbox("Program / Department", sorted(dept_names), index=0)
    with colB: selected_classes = st.multiselect("Classes", STANDARD_CLASSES, default=STANDARD_CLASSES)
    if not selected_classes: st.info("Select at least one class to summarize.")
    else:
        rows = []
        for k in selected_classes:
            class_folders = list_child_folders(dept_id_map[dept]); kf = next((f for f in class_folders if f["name"].strip().lower()==k.strip().lower()), None)
            if not kf: rows.append({"Class": k, "Workbook": "—", "Locked/Total": "0/0", "% Complete": 0.0, "Final Approval":"—"}); continue
            candidates = list_class_spreadsheets(kf["id"])
            if not candidates: rows.append({"Class": k, "Workbook": "—", "Locked/Total": "0/0", "% Complete": 0.0, "Final Approval":"—"}); continue
            prio = [x for x in candidates if x["name"].lower().endswith("_marks")]; meta = prio[0] if prio else candidates[0]; ssid, ssname = meta["id"], meta["name"]
            cfg = _prep_cfg(load_tab(ssid, "_Config")); audit = load_tab(ssid, "_Audit"); locked, total, _ = all_components_locked_for_class(cfg, audit, k)
            pct = round((locked/total*100.0),1) if total else 0.0; approval = "✅ Yes" if is_class_final_approved(ssid, k) else "⏳ No"
            rows.append({"Class": k, "Workbook": ssname, "Locked/Total": f"{locked}/{total}", "% Complete": pct, "Final Approval": approval})
        df = pd.DataFrame(rows); st.subheader(f"Overview — {dept}"); st.markdown('<div class="kpi">', unsafe_allow_html=True); c1,c2,c3 = st.columns(3)
        avg = float(df["% Complete"].mean()) if not df.empty else 0.0; c1.metric("Average Completion", f"{avg:.1f}%"); c2.metric("Approved", f"{int((df['Final Approval']=='✅ Yes').sum())} / {len(df)}"); c3.metric("Classes", f"{len(df)}")
        st.markdown('</div>', unsafe_allow_html=True); st.markdown(""); st.dataframe(df, use_container_width=True, height=320)
        try: st.bar_chart(df[["Class","% Complete"]].set_index("Class"))
        except Exception: st.caption("Chart unavailable for current data.")

elif nav == "Class View":
    colA, colB = st.columns([2,2])
    with colA: dept = st.selectbox("Program / Department", sorted(dept_names), index=0, key="cls_dept")
    with colB: klass = st.selectbox("Class", STANDARD_CLASSES, index=0, key="cls_class")

    ssid, ssname = "", ""
    class_folders = list_child_folders(dept_id_map[dept]); kf = next((f for f in class_folders if f["name"].strip().lower()==klass.strip().lower()), None)
    if not kf: st.info("Selected class folder not found under this department.")
    else:
        candidates = list_class_spreadsheets(kf["id"])
        if not candidates: st.info("No spreadsheet in this class folder.")
        else: prio = [x for x in candidates if x["name"].lower().endswith("_marks")]; meta = prio[0] if prio else candidates[0]; ssid, ssname = meta["id"], meta["name"]

    if ssid:
        st.markdown(f"**Workbook:** `{ssname}`")
        cfg = _prep_cfg(load_tab(ssid, "_Config")); audit = load_tab(ssid, "_Audit"); locked, total, _ = all_components_locked_for_class(cfg, audit, klass)
        c1,c2,c3 = st.columns(3); c1.metric("Components Locked", f"{locked} / {total}"); c2.metric("Final Approval", "✅ Approved" if is_class_final_approved(ssid, klass) else "⏳ Pending"); c3.metric("Lock Cutoff", get_class_cutoff_display(ssid) or "—")
        st.progress(min(100, int((locked/total*100.0) if total else 0)))

        st.subheader("Per-Course Component Status")
        # Get all sheet titles once for efficiency
        all_sheet_titles = list(get_sheet_id_map(ssid).keys())
        tbl = per_course_lock_table(cfg, audit, klass)
        
        if tbl.empty: st.info("No _Config found for this class.")
        else:
            header_cols = st.columns([2, 4, 2, 2])
            header_cols[0].markdown("**Course Code**")
            header_cols[1].markdown("**Course**")
            header_cols[2].markdown("**Component**")
            header_cols[3].markdown("**Status / Action**")
            st.markdown("---")

            for _, row in tbl.iterrows():
                row_cols = st.columns([2, 4, 2, 2])
                row_cols[0].write(row["CourseCode"])
                row_cols[1].write(row["Course"])
                row_cols[2].write(row["Component"])
                
                with row_cols[3]:
                    if row["Status"] == "🔒 Locked":
                        component_name = row["Component"]
                        # --- MODIFIED LOGIC: Find tab name automatically ---
                        marks_tab_name = find_matching_marks_tab(component_name, all_sheet_titles)
                        
                        if marks_tab_name:
                            marks_df = load_tab(ssid, marks_tab_name)
                            if not marks_df.empty:
                                csv_bytes = df_to_csv_bytes(marks_df)
                                file_name = _slug(dept, klass, row['CourseCode'], row['Component'], "Marks") + ".csv"
                                st.download_button(
                                    label="⬇️ Download Marks",
                                    data=csv_bytes,
                                    file_name=file_name,
                                    key=f"dl_{ssid}_{row['CourseCode']}_{row['Component']}"
                                )
                            else:
                                st.caption("Empty Sheet")
                        else:
                            st.caption("Sheet Not Found")
                    else:
                        st.write("🔓 Unlocked")

        # ... (Final/Provisional downloads code is unchanged) ...
        st.subheader("Class Final (Approved / Provisional)")
        st.caption("Final result sheets are loaded from tabs containing the word 'Final'.")

# Footer
st.markdown("---")
st.caption("© SGU Internal Marks Management System — Nilesh Vijay Sabnis")
st.caption(f"Dashboard Version {DASHBOARD_VERSION} | Last Build: {LAST_BUILD_STR}")
