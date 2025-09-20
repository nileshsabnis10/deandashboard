# Dean Dashboard — UI v5.0.0 (Final)
# Built: 20 Sep 2025, 08:00 AM IST
# Notes:
#   • Final version with all features and bug fixes implemented.
#   • Faculty lookup now correctly uses only the Course Code, fixing the "N/A" issue.
#   • Dashboard defaults to "Class View".

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
DASHBOARD_VERSION = "5.0.0"
LAST_BUILD_STR = "20 Sep 2025, 08:00 AM IST"

# ==============================
# Read-only configuration
# ==============================
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

def _secrets():
    try:
        return dict(st.secrets)
    except Exception:
        return {}

def load_settings() -> Dict:
    s = _secrets()
    drive = s.get("drive", {})
    paths = s.get("paths", {})
    parent_folder_id = drive.get("parent_folder_id") or os.getenv("PARENT_FOLDER_ID","").strip()
    if not parent_folder_id:
        st.error("Missing drive.parent_folder_id in secrets.", icon="🚫")
        st.stop()
    standard_classes = drive.get("standard_classes") or ["First Year","Second Year","Third Year","Final Year"]
    read_sa = paths.get("read_service_account_file") or os.getenv("READ_SA_FILE","service-account.json")
    return {
        "PARENT_FOLDER_ID": parent_folder_id,
        "STANDARD_CLASSES": list(standard_classes),
        "READ_SA_FILE": os.path.expanduser(read_sa),
    }

SET = load_settings()
PARENT_FOLDER_ID = SET["PARENT_FOLDER_ID"]
STANDARD_CLASSES = SET["STANDARD_CLASSES"]

# ==============================
# Auth (READ ONLY)
# ==============================
def _creds_from_path_or_json(path_or_json: str):
    if os.path.exists(path_or_json):
        return Credentials.from_service_account_file(path_or_json, scopes=SCOPES)
    try:
        data = json.loads(path_or_json)
        return Credentials.from_service_account_info(data, scopes=SCOPES)
    except Exception:
        raise RuntimeError("READ SA not found. Provide paths.read_service_account_file in secrets or a valid JSON string.")

CREDS = _creds_from_path_or_json(SET["READ_SA_FILE"])
SESSION = AuthorizedSession(CREDS)

# ==============================
# Drive helpers (READ)
# ==============================
def drive_list(q: str, fields: str = "files(id,name,mimeType,parents)") -> List[Dict]:
    url = "https://www.googleapis.com/drive/v3/files"
    params = { "q": q, "fields": fields, "pageSize": 1000, "supportsAllDrives": "true", "includeItemsFromAllDrives": "true" }
    r = SESSION.get(url, params=params, timeout=60); r.raise_for_status()
    return r.json().get("files", [])

def list_child_folders(parent_id: str) -> List[Dict]:
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    return sorted(drive_list(q, "files(id,name)"), key=lambda f: f["name"].lower())

def list_class_spreadsheets(class_folder_id: str) -> List[Dict]:
    q = (f"'{class_folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false")
    return drive_list(q, "files(id,name)")

# ==============================
# Sheets helpers (READ)
# ==============================
def _df_from_values(values: List[List[str]]) -> pd.DataFrame:
    if not values or len(values) <= 1: return pd.DataFrame()
    cols = values[0] if values else []
    rows = values[1:] if len(values) > 1 else []
    try:
        df = pd.DataFrame(rows, columns=cols)
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
    out={}
    for s in r.json().get("sheets", []):
        p=s.get("properties",{})
        out[p.get("title")] = p.get("sheetId")
    return out

# ==============================
# Domain logic
# ==============================
def is_class_final_approved(ssid: str, klass: str) -> bool:
    ap = load_tab(ssid, "_Approvals")
    if ap.empty: return False
    try:
        rows = ap.iloc[1:] if list(ap.columns)[0] == "Scope" else ap
    except Exception:
        rows = ap
    mask = ((rows.iloc[:,0].astype(str).str.strip()=="ClassFinal") & (rows.iloc[:,1].astype(str).str.strip()==klass))
    return bool(mask.any())

def _prep_cfg(cfg: pd.DataFrame) -> pd.DataFrame:
    if cfg.empty: return pd.DataFrame(columns=["Class","Course","CourseCode","Component","MaxMarks","_class_lower","_code_lower","_comp_lower"])
    c = cfg.copy()
    for col in ["Class", "Course", "CourseCode", "Component"]:
        if col not in c.columns: c[col] = ""
    c["_class_lower"] = c["Class"].astype(str).str.strip().str.lower()
    c["_code_lower"]  = c["CourseCode"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.lower()
    c["_comp_lower"]  = c["Component"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.lower()
    return c

def all_components_locked_for_class(cfg: pd.DataFrame, audit: pd.DataFrame, klass: str) -> Tuple[int,int,bool]:
    if cfg.empty: return 0,0,False
    c = cfg[cfg["_class_lower"] == str(klass).lower()].copy()
    total = len(c)
    if total == 0: return 0,0,False
    if audit.empty: return 0,total,False
    au = audit.copy()
    for col in ["Class","Course","Component","Action"]:
        if col not in au.columns: au[col] = ""
    au["key"] = (au["Course"].astype(str).str.lower().str.strip() + "||" + au["Component"].astype(str).str.lower().str.strip())
    c["key"] = c["_code_lower"] + "||" + c["_comp_lower"]
    locked = c["key"].isin(set(au[au["Action"].astype(str).str.lower()=="locked"]["key"])).sum()
    return int(locked), int(total), bool(locked==total)

def per_course_lock_table(ssid: str, cfg: pd.DataFrame, audit: pd.DataFrame, assignments: pd.DataFrame, klass: str) -> pd.DataFrame:
    if cfg.empty: return pd.DataFrame()
    c = cfg[cfg["_class_lower"] == str(klass).lower()].copy()
    if c.empty: return pd.DataFrame()

    a = audit.copy()
    for col in ["Class", "Course", "Component", "Action"]:
        if col not in a.columns: a[col] = ""
    a = a[a["Class"].astype(str).str.strip().str.lower() == str(klass).lower().strip()]
    a["key"] = a["Course"].astype(str).str.lower().str.strip() + "||" + a["Component"].astype(str).str.lower().str.strip()
    locked_keys = set(a[a["Action"].astype(str).str.lower() == "locked"]["key"])

    asm = assignments.copy()
    for col in ["CourseCode", "FacultyID"]:
        if col not in asm.columns: asm[col] = ""
    
    # Create a key from just the course code
    asm["key"] = asm["CourseCode"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.lower()

    def format_faculty_name(email_id):
        s_email = str(email_id).strip()
        if '@' not in s_email:
            return "N/A"
        name_part = s_email.split('@')[0]
        return ' '.join([part.capitalize() for part in name_part.split('.')])
    
    asm["FacultyName"] = asm["FacultyID"].apply(format_faculty_name)
    # Create a map of {course_code -> Faculty Name}
    faculty_map = pd.Series(asm.FacultyName.values, index=asm.key).to_dict()

    statuses = []
    faculties = []
    for index, row in c.iterrows():
        # Status logic
        lock_key = f"{row['_code_lower']}||{row['_comp_lower']}"
        if lock_key in locked_keys:
            statuses.append("🔒 Locked")
        else:
            course_code = row["CourseCode"]
            component = row["Component"]
            data_sheet_name = f"{course_code}__{component}"
            data_df = load_tab(ssid, data_sheet_name)
            if not data_df.empty: statuses.append("📝 Draft Saved")
            else: statuses.append("⚫ Not Started")
        
        # Faculty lookup using only the course code
        course_key = row["_code_lower"]
        faculties.append(faculty_map.get(course_key, "N/A"))
            
    c["Status"] = statuses
    c["Faculty"] = faculties
    
    view = c[["CourseCode", "Course", "Component", "Status", "Faculty"]].drop_duplicates().copy()
    view = view.sort_values(by=["CourseCode", "Component"])
    return view

# --- Robust Final/Provisional detection (unchanged logic) ---
def _norm(s: str) -> str: return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip()
def _klass_variants(klass: str) -> List[str]:
    k = str(klass).strip()
    v = {k, k.replace("/", "-"), k.replace("/", "_"), k.replace(" ", "-"), k.replace(" ", "_"), k.replace(" ", ""),}
    return list(v)

@st.cache_data(ttl=600, show_spinner=False)
def find_final_tabs(ssid: str, klass: str) -> Dict[str, str]:
    title_map = get_sheet_id_map(ssid)
    titles = list(title_map.keys())
    if not titles: return {"approved":"", "provisional":""}

    norms = {t: _norm(t) for t in titles}
    k_norm = _norm(klass)
    k_tokens = set(k_norm.split())
    variants = set(map(_norm, _klass_variants(klass)))
    prov_tokens = {"provisional", "preview", "draft", "prov"}
    exact_candidates = []
    for t in titles:
        nt = norms[t]
        if "final" in nt:
            if any(v in nt for v in variants) or k_tokens.issubset(set(nt.split())):
                exact_candidates.append(t)
    approved, provisional = "", ""
    for t in exact_candidates:
        nt = norms[t]
        if any(tok in nt for tok in prov_tokens):
            if not provisional: provisional = t
        else:
            if not approved: approved = t
        if approved and provisional: break
    if not approved or not provisional:
        for t in titles:
            nt = norms[t]
            if "final" in nt:
                if any(tok in nt for tok in prov_tokens):
                    if not provisional: provisional = t
                else:
                    if not approved: approved = t
            if approved and provisional: break
    if not approved:
        for lit in [f"{klass}__Final", f"{klass} Final", f"Final {klass}", f"Class Final - {klass}", f"{klass} - Class Final"]:
            if lit in titles: approved = lit; break
    if not provisional:
        for lit in [f"{klass}__Final (Provisional)", f"{klass}__Final_Provisional", f"{klass} Final (Provisional)"]:
            if lit in titles: provisional = lit; break
    return {"approved": approved, "provisional": provisional}

# --- Helpers for downloads (Excel + CSV) ---
def _slug(*parts: str) -> str:
    s = " ".join(map(str, parts))
    s = re.sub(r"[^\w\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "export"

def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Class Final") -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf) as writer:
        (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_excel(writer, index=False, sheet_name=sheet_name[:31] or "Sheet1")
    buf.seek(0)
    return buf.read()

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_csv(index=False).encode("utf-8-sig")

# ==============================
# UI — main-screen nav & filters (no sidebar)
# ==============================
st.set_page_config(page_title="Dean's Dashboard — Read Only", layout="wide", page_icon="📊")

# Top bar + Title
st.markdown("""<style>.topbar {height: 6px; background: linear-gradient(90deg, #e11d48, #f59e0b, #22c55e, #3b82f6); border-radius: 8px; margin-bottom: 10px;}.kpi {border-radius: 14px; padding: 16px; border: 1px solid rgba(0,0,0,0.06); box-shadow: 0 2px 6px rgba(0,0,0,.04); background: #fff;}.muted {color: #6b7280; font-size: 12px;}</style><div class="topbar"></div>""", unsafe_allow_html=True)
title_col, meta_col = st.columns([3,1])
with title_col:
    st.title("📊 Progress Dashboard")
    st.caption("Read-only visibility into internal marks submission and approvals.")
with meta_col:
    st.metric("Version", DASHBOARD_VERSION)
    st.caption(f"Last Build: {LAST_BUILD_STR}")

# NAV on main screen (Health removed)
nav = st.radio("View", ["Class View", "Overview"], horizontal=True)

# Common: department list
dept_folders = list_child_folders(PARENT_FOLDER_ID)
dept_names = [f["name"] for f in dept_folders]
if not dept_names:
    st.error("No department folders found under the parent.", icon="🚫")
    st.stop()
dept_id_map = {f["name"]: f["id"] for f in dept_folders}

if nav == "Overview":
    colA, colB = st.columns([2,2])
    with colA: dept = st.selectbox("Program / Department", sorted(dept_names), index=0)
    with colB: selected_classes = st.multiselect("Classes", STANDARD_CLASSES, default=STANDARD_CLASSES)

    if not selected_classes: st.info("Select at least one class to summarize.")
    else:
        rows = []
        for k in selected_classes:
            class_folders = list_child_folders(dept_id_map[dept])
            kf = next((f for f in class_folders if f["name"].strip().lower()==k.strip().lower()), None)
            if not kf:
                rows.append({"Class": k, "Workbook": "—", "Locked/Total": "0/0", "% Complete": 0.0, "Final Approval":"—"}); continue
            candidates = list_class_spreadsheets(kf["id"])
            if not candidates:
                rows.append({"Class": k, "Workbook": "—", "Locked/Total": "0/0", "% Complete": 0.0, "Final Approval":"—"}); continue
            prio = [x for x in candidates if x["name"].lower().endswith("_marks")]
            meta = prio[0] if prio else candidates[0]
            ssid, ssname = meta["id"], meta["name"]
            cfg = _prep_cfg(load_tab(ssid, "_Config"))
            audit = load_tab(ssid, "_Audit")
            locked, total, _ = all_components_locked_for_class(cfg, audit, k)
            pct = round((locked/total*100.0),1) if total else 0.0
            approval = "✅ Yes" if is_class_final_approved(ssid, k) else "⏳ No"
            rows.append({"Class": k, "Workbook": ssname, "Locked/Total": f"{locked}/{total}", "% Complete": pct, "Final Approval": approval})
        df = pd.DataFrame(rows)

        st.subheader(f"Overview — {dept}")
        st.markdown('<div class="kpi">', unsafe_allow_html=True)
        c1,c2,c3 = st.columns(3)
        avg = float(df["% Complete"].mean()) if not df.empty else 0.0
        c1.metric("Average Completion", f"{avg:.1f}%")
        c2.metric("Approved", f"{int((df['Final Approval']=='✅ Yes').sum())} / {len(df)}")
        c3.metric("Classes", f"{len(df)}")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("")
        st.dataframe(df, use_container_width=True, height=320)
        try:
            st.bar_chart(df[["Class","% Complete"]].set_index("Class"))
        except Exception: st.caption("Chart unavailable for current data.")

elif nav == "Class View":
    colA, colB = st.columns([2,2])
    with colA: dept = st.selectbox("Program / Department", sorted(dept_names), index=0, key="cls_dept")
    with colB: klass = st.selectbox("Class", STANDARD_CLASSES, index=0, key="cls_class")

    ssid, ssname = "", ""
    class_folders = list_child_folders(dept_id_map[dept])
    kf = next((f for f in class_folders if f["name"].strip().lower()==klass.strip().lower()), None)
    if not kf: st.info("Selected class folder not found under this department.")
    else:
        candidates = list_class_spreadsheets(kf["id"])
        if not candidates: st.info("No spreadsheet in this class folder.")
        else:
            prio = [x for x in candidates if x["name"].lower().endswith("_marks")]
            meta = prio[0] if prio else candidates[0]
            ssid, ssname = meta["id"], meta["name"]

    if ssid:
        st.markdown(f"**Workbook:** `{ssname}`")
        cfg = _prep_cfg(load_tab(ssid, "_Config"))
        audit = load_tab(ssid, "_Audit")
        assignments = load_tab(ssid, "_Assignments")
        
        locked, total, _ = all_components_locked_for_class(cfg, audit, klass)
        c1, c2 = st.columns(2)
        c1.metric("Components Locked", f"{locked} / {total}")
        c2.metric("Final Approval", "✅ Approved" if is_class_final_approved(ssid, klass) else "⏳ Pending")
        
        st.progress(min(100, int((locked/total*100.0) if total else 0)))

        st.subheader("Per-Course Component Status")
        tbl = per_course_lock_table(ssid, cfg, audit, assignments, klass)
        if tbl.empty: st.info("No _Config found for this class.")
        else: st.dataframe(tbl, use_container_width=True, height=420)

        st.subheader("Class Final (Approved / Provisional)")
        ft = find_final_tabs(ssid, klass)
        approved_title, provisional_title = ft.get("approved") or "", ft.get("provisional") or ""

        if approved_title:
            st.markdown(f"**Approved Final:** `{approved_title}`")
            final_df = load_tab(ssid, approved_title)
            if final_df.empty: st.caption(f"`{approved_title}` loaded but has no visible rows in A1:ZZZ.")
            else:
                st.dataframe(final_df, use_container_width=True, height=360)
                try:
                    xbytes, cbytes = df_to_excel_bytes(final_df, sheet_name="Approved Final"), df_to_csv_bytes(final_df)
                    fname_x, fname_c = _slug(dept, klass, "Approved_Final")+".xlsx", _slug(dept, klass, "Approved_Final")+".csv"
                    d1, d2 = st.columns(2)
                    with d1: d1.download_button("⬇️ Excel (Approved)", data=xbytes, file_name=fname_x, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"x_approved_{ssid}_{klass}")
                    with d2: d2.download_button("⬇️ CSV (Approved)", data=cbytes, file_name=fname_c, mime="text/csv", key=f"c_approved_{ssid}_{klass}")
                except Exception as e: st.caption(f"Could not prepare downloads: {e}")
        else: st.caption("Approved Final not found.")

        if provisional_title:
            st.markdown(f"**Provisional Final:** `{provisional_title}`")
            prov_df = load_tab(ssid, provisional_title)
            if prov_df.empty: st.caption(f"`{provisional_title}` loaded but has no visible rows in A1:ZZZ.")
            else:
                st.dataframe(prov_df, use_container_width=True, height=360)
                try:
                    xbytes, cbytes = df_to_excel_bytes(prov_df, sheet_name="Provisional Final"), df_to_csv_bytes(prov_df)
                    fname_x, fname_c = _slug(dept, klass, "Provisional_Final")+".xlsx", _slug(dept, klass, "Provisional_Final")+".csv"
                    d1, d2 = st.columns(2)
                    with d1: d1.download_button("⬇️ Excel (Provisional)", data=xbytes, file_name=fname_x, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"x_provisional_{ssid}_{klass}")
                    with d2: d2.download_button("⬇️ CSV (Provisional)", data=cbytes, file_name=fname_c, mime="text/csv", key=f"c_provisional_{ssid}_{klass}")
                except Exception as e: st.caption(f"Could not prepare downloads: {e}")
        elif not approved_title: st.caption("No Provisional Final found either.")

# Footer
st.markdown("---")
st.caption("© SGU Internal Marks Management System — Nilesh Vijay Sabnis")
st.caption(f"Dashboard Version {DASHBOARD_VERSION} | Last Build: {LAST_BUILD_STR}")
