
import imaplib
import os
import time
import email
from email.header import decode_header
from dotenv import load_dotenv
import pandas as pd
from io import StringIO

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from hot_order_agent_core.nlp import detect_intents, extract_order_id
from hot_order_agent_core.hoa import process_single_order

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
INCOMING_DIR = os.path.join(DATA_DIR, "incoming")
MASTER_CSV = os.path.join(DATA_DIR, "sample_orders.csv")

def env(key, default=None, cast=str):
    v = os.getenv(key, default)
    if v is None:
        return None
    if cast is int:
        try:
            return int(v)
        except:
            return default
    return v

def connect():
    host = env("IMAP_HOST", "imap.gmail.com")
    port = env("IMAP_PORT", 993, int)
    user = env("IMAP_USER")
    pwd = env("IMAP_PASSWORD")
    if not (user and pwd):
        raise RuntimeError("IMAP_USER or IMAP_PASSWORD missing. Set them in .env")
    M = imaplib.IMAP4_SSL(host, port)
    M.login(user, pwd)
    folder = env("IMAP_FOLDER", "INBOX")
    M.select(folder)
    return M

def normalize_subject(raw):
    if raw is None:
        return ""
    parts = decode_header(raw)
    out = ""
    for text, enc in parts:
        if isinstance(text, bytes):
            out += text.decode(enc or "utf-8", errors="ignore")
        else:
            out += text
    return out

def get_sender_email(msg):
    from_hdr = msg.get("From") or ""
    import re
    m = re.search(r'<([^>]+)>', from_hdr)
    if m:
        return m.group(1)
    m = re.search(r'([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})', from_hdr)
    return m.group(1) if m else None

def parse_body_as_csv(msg):
    if msg.is_multipart():
        for part in msg.walk():
            disp = str(part.get("Content-Disposition") or "")
            ctype = part.get_content_type()
            if "attachment" in disp.lower() or ctype == "text/csv":
                payload = part.get_payload(decode=True)
                if payload:
                    try:
                        s = payload.decode(part.get_content_charset() or "utf-8", errors="ignore")
                        return pd.read_csv(StringIO(s))
                    except Exception:
                        pass
            if ctype in ("text/plain", "text/csv"):
                payload = part.get_payload(decode=True)
                if payload:
                    try:
                        s = payload.decode(part.get_content_charset() or "utf-8", errors="ignore")
                        return pd.read_csv(StringIO(s))
                    except Exception:
                        pass
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            try:
                s = payload.decode(msg.get_content_charset() or "utf-8", errors="ignore")
                return pd.read_csv(StringIO(s))
            except Exception:
                pass
    return None

def append_to_master(new_df, fallback_email=None):
    required = ["order_id","product","qty","customer","priority","origin","destination","customer_email"]
    for col in required:
        if col not in new_df.columns:
            new_df[col] = None
    if fallback_email is not None:
        if 'customer_email' in new_df.columns:
            new_df['customer_email'] = new_df['customer_email'].fillna(fallback_email).replace('', fallback_email)
        else:
            new_df['customer_email'] = fallback_email
    if "qty" in new_df.columns:
        new_df["qty"] = pd.to_numeric(new_df["qty"], errors="coerce").fillna(0).astype(int)

    if os.path.exists(MASTER_CSV):
        master = pd.read_csv(MASTER_CSV)
        combined = pd.concat([master, new_df], ignore_index=True)
        if "order_id" in combined.columns:
            combined = combined.drop_duplicates(subset=["order_id"], keep="last")
        if "order_id" in combined.columns:
            combined["order_id"] = combined["order_id"].astype(str).str.strip()
    else:
        combined = new_df

    combined.to_csv(MASTER_CSV, index=False)
    print(f"Updated {MASTER_CSV} with {len(new_df)} new rows. Total rows: {len(combined)}.")

def get_plaintext(msg):
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition") or "")
            if "attachment" in disp.lower():
                continue
            if ctype == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    try:
                        return payload.decode(part.get_content_charset() or "utf-8", errors="ignore")
                    except Exception:
                        continue
    payload = msg.get_payload(decode=True)
    if payload:
        try:
            return payload.decode(msg.get_content_charset() or "utf-8", errors="ignore")
        except Exception:
            pass
    return ""

def find_latest_order_for_sender(sender_email):
    try:
        if not os.path.exists(MASTER_CSV):
            return None
        df = pd.read_csv(MASTER_CSV)
        if "customer_email" not in df.columns or df.empty:
            return None
        sub = df[df["customer_email"].astype(str).str.lower() == str(sender_email or "").lower()]
        if sub.empty:
            return None
        return str(sub.iloc[-1]["order_id"])
    except Exception:
        return None

def process_message(M, num):
    res, data = M.fetch(num, "(RFC822)")
    if res != "OK":
        print(f"Failed to fetch message {num}")
        return

    msg = email.message_from_bytes(data[0][1])
    subject = normalize_subject(msg.get("Subject"))
    sender_email = get_sender_email(msg)
    body_text = get_plaintext(msg)

    print(f"Processing message {num.decode()} from {sender_email} subject: {subject}")

    df = parse_body_as_csv(msg)
    if df is not None and not df.empty:
        # --- normalize columns & ensure order_id ---
        df.columns = [str(c).strip().lower() for c in df.columns]

        if "order_id" not in df.columns:
            for cand in ["id", "po", "po_id", "orderid", "order no", "order_no", "order"]:
                if cand in df.columns:
                    df["order_id"] = df[cand]
                    break

        if "order_id" not in df.columns:
            extracted = extract_order_id(subject) or extract_order_id(body_text)
            if extracted:
                df["order_id"] = extracted

        if "order_id" not in df.columns:
            print("No 'order_id' column found; auto-generating IDs.")
            next_id = 7000
            try:
                if os.path.exists(MASTER_CSV):
                    m = pd.read_csv(MASTER_CSV)
                    if "order_id" in m.columns and not m.empty:
                        m_ids = pd.to_numeric(m["order_id"], errors="coerce")
                        if m_ids.notna().any():
                            next_id = int(m_ids.max()) + 1
            except Exception:
                pass
            df["order_id"] = [str(next_id + i) for i in range(len(df))]

        df["order_id"] = df["order_id"].astype(str).str.strip()
        if "qty" in df.columns:
            df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
        if "customer_email" not in df.columns:
            df["customer_email"] = sender_email

        df = df[df["order_id"] != ""]
        if df.empty:
            print("CSV contained no valid rows after normalization; skipping.")
        else:
            append_to_master(df, fallback_email=sender_email)
            try:
                df["order_id"] = df["order_id"].astype(str).str.strip()
                for _, row in df.iterrows():
                    oid = str(row["order_id"]).strip()
                    overrides = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                    overrides["customer_email"] = overrides.get("customer_email") or sender_email
                    process_single_order(oid, overrides=overrides)
            except Exception as e:
                print("Error processing new orders:", e)
    else:
        order_id = extract_order_id(subject) or extract_order_id(body_text)
        if not order_id:
            order_id = find_latest_order_for_sender(sender_email)
            if not order_id:
                print("No order_id found in reply; skipping.")
                try:
                    M.store(num, "+FLAGS", "\\\\Seen")
                except Exception:
                    pass
                return
            else:
                print(f"Fallback matched sender to order_id {order_id}")

        intents = detect_intents(body_text + " " + subject)
        overrides = {}
        if intents.get("expedite_request"):
            overrides["priority"] = "High"
        if intents.get("change_qty") is not None:
            overrides["qty"] = intents["change_qty"]
        if intents.get("change_destination"):
            overrides["destination"] = intents["change_destination"]
        if intents.get("cancel_order"):
            overrides["qty"] = 0
            overrides["priority"] = "Normal"
        overrides["customer_email"] = sender_email

        try:
            result = process_single_order(order_id, overrides=overrides)
            print("Recomputed:", result)
        except Exception as e:
            print(f"Failed to process order {order_id}: {e}")

    try:
        M.store(num, "+FLAGS", "\\\\Seen")
    except Exception:
        pass

def main_loop():
    load_dotenv()
    subject_filter = (os.getenv("IMAP_SEARCH_SUBJECT") or "").strip()
    poll_seconds = int(os.getenv("IMAP_POLL_SECONDS", "15"))

    while True:
        try:
            M = connect()
            if subject_filter:
                status, data = M.search(None, f'(UNSEEN SUBJECT "{subject_filter}")')
                ids = data[0].split() if status == "OK" else []
                if not ids:
                    status, data = M.search(None, '(UNSEEN)')
            else:
                status, data = M.search(None, '(UNSEEN)')

            if status == "OK":
                ids = data[0].split()
                if ids:
                    print(f"Found {len(ids)} unread emails.")
                    for num in ids:
                        process_message(M, num)
                else:
                    print("No new emails.")
            else:
                print("Search failed:", status)

            M.logout()
        except Exception as e:
            print("Error in polling loop:", e)

        time.sleep(max(5, poll_seconds))

if __name__ == "__main__":
    main_loop()
