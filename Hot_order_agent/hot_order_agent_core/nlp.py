
import re

KEYWORDS_EXPEDITE = [
    "expedite", "expedited", "faster", "fastest", "urgent", "asap",
    "rush", "priority", "ship sooner", "ship today", "tomorrow", "next day", "1 day", "one day"
]
KEYWORDS_CANCEL = ["cancel", "void", "drop the order", "stop this order", "do not ship"]
KEYWORDS_CONFIRM = ["confirm", "approved", "go ahead", "proceed", "looks good"]

def detect_intents(text: str):
    t = (text or "").lower()
    intents = {
        "expedite_request": any(k in t for k in KEYWORDS_EXPEDITE),
        "cancel_order": any(k in t for k in KEYWORDS_CANCEL),
        "confirm": any(k in t for k in KEYWORDS_CONFIRM),
        "change_qty": None,
        "change_destination": None,
    }
    m_qty = re.search(r'(?:qty|quantity)\s*[:=\-\s]*([0-9]{1,6})', t)
    if m_qty:
        try:
            intents["change_qty"] = int(m_qty.group(1))
        except:
            pass

    m_dest = re.search(r'(?:destination|ship to|to)\s*[:=\-\s]*([a-zA-Z\-\s]{2,40})', t)
    if m_dest:
        intents["change_destination"] = m_dest.group(1).strip()

    m_days = re.search(r'(?:in|within|by)\s*([0-9]{1,2})\s*day', t)
    intents["desired_days"] = int(m_days.group(1)) if m_days else None
    return intents

def extract_order_id(text: str):
    if not text:
        return None
    pats = [
        r'\b(?:order|po|ord\s*#|order\s*#|po\s*#)\s*[:#-]?\s*([0-9]{3,})\b',
        r'\bID\s*[:#-]?\s*([0-9]{3,})\b',
    ]
    for p in pats:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    m = re.search(r'order[_\s-]?id[^\n\r]*?([0-9]{3,})', text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'\b([0-9]{3,})\b', text)
    return m.group(1) if m else None
