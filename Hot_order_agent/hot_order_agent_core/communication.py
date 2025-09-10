import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from dotenv import load_dotenv

LOG_PATH = "logs/communication.log"

def _env_bool(key, default=False):
    v = (os.getenv(key, str(default)) or str(default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

def _sanitize_email(val):
    try:
        from math import isnan
        if val is None:
            return None
        if isinstance(val, float):
            try:
                if isnan(val):
                    return None
            except Exception:
                pass
        s = str(val).strip()
        return s if s else None
    except Exception:
        return None

def _send_email(to_email: str, subject: str, html_body: str, cc_email: str = None, reply_to: str = None):
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "465"))
    user = os.getenv("SMTP_USER")
    pwd = os.getenv("SMTP_PASSWORD")
    use_tls = _env_bool("SMTP_USE_TLS", False)
    from_header = os.getenv("EMAIL_FROM", user or "hotorderagent@example.com")

    if not (user and pwd):
        raise RuntimeError("SMTP_USER and/or SMTP_PASSWORD not set. Configure SMTP in .env")

    msg = EmailMessage()
    msg["From"] = from_header
    msg["To"] = to_email
    if cc_email:
        msg["Cc"] = cc_email
    if reply_to:
        msg["Reply-To"] = reply_to
    msg["Subject"] = subject
    msg.set_content("This is an HTML email. Please view in an HTML-capable client.")
    msg.add_alternative(html_body, subtype="html")

    recipients = [to_email] + ([cc_email] if cc_email else [])

    if use_tls:
        with smtplib.SMTP(host, port) as s:
            s.ehlo()
            s.starttls()
            s.login(user, pwd)
            s.send_message(msg, to_addrs=recipients)
    else:
        with smtplib.SMTP_SSL(host, port) as s:
            s.login(user, pwd)
            s.send_message(msg, to_addrs=recipients)

def _log(line: str):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")

def send_customer_update(order_id, status, dc, cost, eta, available_qty, customer, customer_email=None):
    """
    Sends an email to the customer and optionally CCs the CEO; logs the event.
    - To: customer_email (or DEFAULT_CUSTOMER_EMAIL)
    - CC: CEO_EMAIL (if set)
    - Reply-To: defaults to the customer's email (or env override REPLY_TO_EMAIL if set)
    """
    load_dotenv()
    to_email = _sanitize_email(customer_email) or os.getenv("DEFAULT_CUSTOMER_EMAIL")
    ceo_email = os.getenv("CEO_EMAIL")  # leave empty in .env to disable CC
    reply_to_override = os.getenv("REPLY_TO_EMAIL")
    reply_to = reply_to_override or to_email

    if not to_email:
        _log(f"[{datetime.utcnow().isoformat()}Z] order={order_id} customer={customer} status={status} dc={dc} available_qty={available_qty} expedite_cost=${cost} eta_days={eta} (email skipped; DEFAULT_CUSTOMER_EMAIL not set)")
        return

    subject = f"[Hot Order Agent] Update for {customer} — Order {order_id}"
    html = f"""
    <html>
    <body>
      <p>Dear {customer},</p>
      <p>Here is the latest status for your order <b>{order_id}</b>:</p>
      <ul>
        <li>Status: <b>{status}</b></li>
        <li>Selected DC: <b>{dc}</b></li>
        <li>Available Qty: <b>{available_qty}</b></li>
        <li>Expedite Cost: <b>${cost}</b></li>
        <li>Estimated Ship Days: <b>{eta}</b></li>
      </ul>
      <p>Best regards,<br/>Hot Order Agent</p>
    </body>
    </html>
    """

    try:
        _send_email(to_email, subject, html, cc_email=ceo_email, reply_to=reply_to)
        _log(f"[{datetime.utcnow().isoformat()}Z] order={order_id} customer={customer} status={status} dc={dc} available_qty={available_qty} expedite_cost=${cost} eta_days={eta} sent_to={to_email} cc={ceo_email} reply_to={reply_to}")
    except Exception as e:
        _log(f"[{datetime.utcnow().isoformat()}Z] order={order_id} customer={customer} status={status} dc={dc} available_qty={available_qty} expedite_cost=${cost} eta_days={eta} EMAIL_ERROR={e}")
