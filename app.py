import io
import re
import json
from datetime import date, timedelta
from typing import Dict, Any, Optional, List

import streamlit as st
import pandas as pd
from rapidfuzz import process, fuzz
import phonenumbers
from email_validator import validate_email, EmailNotValidError

# ============== CONFIG ==============
st.set_page_config(page_title="Koala Courier Export", page_icon="📦", layout="wide")

LMT_COLUMNS = [
    "Nombre",
    "Apellido",
    "Teléfono",
    "Dirección",
    "Comuna",
    "Indicaciones",
    "ID Interno",
    "Correo",
    "Contenido del paquete",
    "cantidad de bultos",
]

DEFAULTS = {
    "contenido_paquete": "Gorros clínicos Koala Scrubs",
    "usar_llm": False,
    "umbral_coincidencia_comuna": 85,
}

# Comunas (resumen; amplía cuando quieras)
COMUNAS_CHILE = [
    "Cerrillos","Cerro Navia","Conchalí","El Bosque","Estación Central","Huechuraba",
    "Independencia","La Cisterna","La Florida","La Granja","La Pintana","La Reina",
    "Las Condes","Lo Barnechea","Lo Espejo","Lo Prado","Macul","Maipú","Ñuñoa",
    "Pedro Aguirre Cerda","Peñalolén","Providencia","Pudahuel","Quilicura","Quinta Normal",
    "Recoleta","Renca","San Joaquín","San Miguel","San Ramón","Santiago","Vitacura",
    "Valparaíso","Viña del Mar","Quilpué","Villa Alemana","Quillota","Concón",
    "Concepción","Talcahuano","San Pedro de la Paz","Coronel","Chiguayante",
    "Antofagasta","Iquique","Arica","La Serena","Coquimbo","Rancagua","Talca",
    "Temuco","Valdivia","Puerto Montt","Punta Arenas"
]

# ===== Helpers =====
def _norm(s: str) -> str:
    import unicodedata
    s2 = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s2).strip().lower()

def match_comuna(raw: str, threshold: int = 85) -> str:
    if not raw:
        return ""
    normalized = _norm(raw)
    candidates = {c: _norm(c) for c in COMUNAS_CHILE}
    for k,v in candidates.items():
        if v == normalized:
            return k
    best = process.extractOne(normalized, list(candidates.values()), scorer=fuzz.WRatio)
    if best and best[1] >= threshold:
        idx = list(candidates.values()).index(best[0])
        return list(candidates.keys())[idx]
    return raw

def normalize_phone_cl(phone_raw: str) -> str:
    if not phone_raw:
        return ""
    s = re.sub(r"[^\d+]", "", str(phone_raw))
    try:
        num = phonenumbers.parse(s, "CL")
        if phonenumbers.is_possible_number(num) and phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
    except Exception:
        pass
    if s.startswith("56"):
        if not s.startswith("+"):
            s = "+" + s
    elif s.startswith("0"):
        s = "+56" + s.lstrip("0")
    elif not s.startswith("+"):
        if len(s) == 9:
            s = "+569" + s[-8:] if not s.startswith("9") else "+56" + s
        else:
            s = "+56" + s
    return s

def validate_email_safe(email_raw: str) -> str:
    if not email_raw:
        return ""
    try:
        v = validate_email(email_raw, check_deliverability=False)
        return v.email
    except EmailNotValidError:
        return email_raw

def split_name(full_name: str) -> (str, str):
    if not full_name:
        return "", ""
    parts = re.split(r"\s+", full_name.strip())
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]

def _extract_unit_note(direccion: str):
    """Separar unidad al final (depto/oficina) → (base, 'Depto 1205' / 'Of. 206')."""
    if not direccion:
        return "", ""
    s = direccion.strip()
    pat = re.compile(
        r"""^(?P<base>.*?)(?:[,;\- ]+)?(?:(?P<tag>depto|dpto|departamento|oficina|of\.?|of)\s*(?P<num>[A-Za-z0-9\-]+))\s*$""",
        re.IGNORECASE
    )
    m = pat.match(s)
    if m:
        base = m.group('base').strip(' ,;-')
        tag = m.group('tag').lower().replace('departamento','depto')
        tag_norm = 'Depto' if tag in ('depto','dpto') else 'Of.'
        num = m.group('num')
        return base, f"{tag_norm} {num}"
    return s, ""

def to_excel_template(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Listado de Direcciones", index=False)
        writer.book["Listado de Direcciones"].sheet_properties.tabColor = "00BFA6"
    buffer.seek(0)
    return buffer.getvalue()

def _autogen_id_interno(phone: str) -> str:
    import random, string
    letters = "".join(random.choice(string.ascii_letters) for _ in range(2))
    last4 = re.sub(r"\D", "", phone or "")[-4:] or "0000"
    return f"{letters}{last4}"

# ===== Parser de TEXTO (IA opcional) =====
def extract_by_labels(text: str) -> Dict[str, str]:
    patterns = {
        "nombre": r"(?:^|\n)\s*(?:nombre|razon\s*social)\s*:\s*(.+)",
        "telefono": r"(?:^|\n)\s*(?:telefono|tel|celular|m[oó]vil)\s*:\s*([^\n]+)",
        "correo": r"(?:^|\n)\s*(?:correo|email|e-?mail)\s*:\s*([^\n]+)",
        "direccion": r"(?:^|\n)\s*(?:direcci[oó]n)\s*:\s*(.+)",
        "comuna": r"(?:^|\n)\s*(?:comuna)\s*:\s*(.+)",
        "indicaciones": r"(?:^|\n)\s*(?:indicaciones|notas)\s*:\s*(.+)",
        "id_interno": r"(?:^|\n)\s*(?:id\s*interno|orden|po|referencia)\s*:\s*(.+)",
    }
    out = {}
    for k, pat in patterns.items():
        m = re.search(pat, text, flags=re.IGNORECASE)
        out[k] = m.group(1).strip() if m else ""
    return out

def llm_parse_block(block: str) -> Optional[Dict[str, Any]]:
    """Usa OpenAI solo para TEXTO pegado."""
    if not st.secrets.get("openai_api_key"):
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=st.secrets["openai_api_key"])
        # Evitamos choque de comillas: construimos el prompt concatenando
        prompt = (
            "Eres un parser estricto. Extrae del texto los campos en JSON con estas claves:\n"
            "- nombre_completo\n- telefono\n- correo\n- direccion\n- comuna\n- indicaciones\n- id_interno\n\n"
            "Responde SOLO JSON válido sin comentarios.\n\nTexto:\n"
            "'''"
            + block +
            "'''"
        )
        resp = client.chat.completions.create(
            model=st.secrets.get("openai_model", "gpt-4o-mini"),
            messages=[
                {"role":"system","content":"Devuelve únicamente JSON válido. Sin explicaciones."},
                {"role":"user","content": prompt}
            ],
            temperature=0
        )
        content = resp.choices[0].message.content.strip()
        data = json.loads(content)

        nombre, apellido = split_name(data.get("nombre_completo",""))
        base_dir, unit_note = _extract_unit_note(data.get("direccion","").strip())
        indic = data.get("indicaciones","").strip()
        if unit_note:
            indic = (indic + ("; " if indic else "") + unit_note)

        return {
            "Nombre": nombre,
            "Apellido": apellido,
            "Teléfono": data.get("telefono",""),
            "Dirección": base_dir,
            "Comuna": data.get("comuna",""),
            "Indicaciones": indic,
            "ID Interno": data.get("id_interno",""),
            "Correo": data.get("correo",""),
        }
    except Exception as e:
        st.warning(f"Parser LLM falló: {e}")
        return None

def parse_block_rule_based(block: str) -> Dict[str, Any]:
    data = extract_by_labels(block)
    nombre_raw = data.get("nombre") or ""
    correo_raw = data.get("correo") or ""
    telefono_raw = data.get("telefono") or ""
    direccion_raw = data.get("direccion") or ""
    comuna_raw = data.get("comuna") or ""
    indic_raw = data.get("indicaciones") or ""
    id_int_raw = data.get("id_interno") or ""

    nombre, apellido = split_name(nombre_raw)
    correo = validate_email_safe(correo_raw)
    telefono = normalize_phone_cl(telefono_raw)
    comuna = match_comuna(comuna_raw, threshold=st.session_state.get("umbral_coincidencia_comuna", DEFAULTS["umbral_coincidencia_comuna"]))

    base_dir, unit_note = _extract_unit_note(direccion_raw.strip())
    indic = indic_raw.strip()
    if unit_note:
        indic = (indic + ("; " if indic else "") + unit_note)

    return {
        "Nombre": nombre,
        "Apellido": apellido,
        "Teléfono": telefono,
        "Dirección": base_dir,
        "Comuna": comuna,
        "Indicaciones": indic,
        "ID Interno": id_int_raw.strip(),
        "Correo": correo,
    }

def process_text(text: str, use_llm: bool, contenido_paquete: str) -> pd.DataFrame:
    blocks = re.split(r"\n\s*\n|^-{3,}$", text.strip(), flags=re.MULTILINE)
    rows = []
    for block in [b for b in blocks if b.strip()]:
        data = llm_parse_block(block) if use_llm else parse_block_rule_based(block)
        data["Teléfono"] = normalize_phone_cl(data.get("Teléfono",""))
        data["Correo"] = validate_email_safe(data.get("Correo",""))
        data["Comuna"] = match_comuna(data.get("Comuna",""))
        data["Contenido del paquete"] = contenido_paquete
        data["cantidad de bultos"] = 1  # valor inicial, luego editable en la tabla
        if not data.get("ID Interno"):
            data["ID Interno"] = _autogen_id_interno(data.get("Teléfono",""))
        rows.append({col: data.get(col, "") for col in LMT_COLUMNS})
    return pd.DataFrame(rows, columns=LMT_COLUMNS)

# ===== Importador JUMPSELLER (mecánico, sin IA) =====
def fetch_jumpseller_orders(start: date, end: date, statuses: List[str], page_limit: int = 250) -> List[Dict[str, Any]]:
    if not (st.secrets.get("jumpseller_store") and st.secrets.get("jumpseller_api_key") and st.secrets.get("jumpseller_api_password")):
        st.error("Faltan secrets de Jumpseller: jumpseller_store / jumpseller_api_key / jumpseller_api_password")
        return []
    import requests
    store = st.secrets["jumpseller_store"].strip()
    base = f"https://{store}.jumpseller.com/api"
    auth = (st.secrets["jumpseller_api_key"], st.secrets["jumpseller_api_password"])
    params = {
        "created_at_min": start.isoformat(),
        "created_at_max": (end + timedelta(days=1)).isoformat(),  # inclusivo
        "page": 1,
        "limit": page_limit
    }
    if statuses:
        params["status"] = ",".join(statuses)

    orders = []
    while True:
        r = requests.get(f"{base}/orders.json", params=params, auth=auth, timeout=30)
        if r.status_code != 200:
            st.error(f"Error Jumpseller {r.status_code}: {r.text[:300]}")
            break
        data = r.json()
        items = data if isinstance(data, list) else data.get("orders", [])
        if not items:
            break
        orders.extend(items)
        if len(items) < page_limit:
            break
        params["page"] += 1
        if params["page"] > 50:
            break
    return orders

def map_jumpseller_to_rows(orders: List[Dict[str, Any]], contenido_paquete: str) -> pd.DataFrame:
    rows = []
    for o in orders:
        shipping = o.get("shipping_address", {}) or {}
        customer = o.get("customer", {}) or {}

        full_name = shipping.get("name") or customer.get("name") or ""
        nombre, apellido = split_name(full_name)

        phone = shipping.get("phone") or customer.get("phone") or ""
        email = customer.get("email") or shipping.get("email") or ""

        direccion_raw = " ".join([p for p in [shipping.get("address"), shipping.get("address_2")] if p]) or ""
        base_dir, unit_note = _extract_unit_note(direccion_raw)

        comuna = shipping.get("city") or shipping.get("province") or ""
        comuna = match_comuna(comuna)

        indicaciones = unit_note
        ref = shipping.get("reference") or o.get("note") or ""
        if ref:
            indicaciones = (indicaciones + ("; " if indicaciones else "") + str(ref).strip())

        telefono_fmt = normalize_phone_cl(phone)
        correo_fmt = validate_email_safe(email)

        row = {
            "Nombre": nombre,
            "Apellido": apellido,
            "Teléfono": telefono_fmt,
            "Dirección": base_dir,
            "Comuna": comuna,
            "Indicaciones": indicaciones,
            "ID Interno": _autogen_id_interno(telefono_fmt),
            "Correo": correo_fmt,
            "Contenido del paquete": contenido_paquete,
            "cantidad de bultos": 1,  # editable después
        }
        rows.append({col: row.get(col, "") for col in LMT_COLUMNS})
    return pd.DataFrame(rows, columns=LMT_COLUMNS)

# ================= UI =================
st.title("📦 Koala Courier Export")
st.caption("Convierte TEXTO o pedidos de Jumpseller en Excel listo para el courier (Plantilla LMT).")

with st.sidebar:
    st.header("Ajustes")
    contenido_paquete = st.text_input("Contenido del paquete", DEFAULTS["contenido_paquete"])
    usar_llm = st.toggle("Usar IA (OpenAI) para parseo de TEXTO", value=DEFAULTS["usar_llm"])
    st.session_state["umbral_coincidencia_comuna"] = st.slider("Umbral de coincidencia comuna", 50, 100, DEFAULTS["umbral_coincidencia_comuna"])
    st.divider()
    st.markdown("**Estado API**")
    st.write("- OpenAI:", "✅ Detectada" if st.secrets.get("openai_api_key") else "—")
    jumpseller_ok = all([st.secrets.get("jumpseller_store"), st.secrets.get("jumpseller_api_key"), st.secrets.get("jumpseller_api_password")])
    st.write("- Jumpseller:", "✅ Listo" if jumpseller_ok else "—")

tab1, tab2, tab3 = st.tabs(["Pegar texto (IA)", "Subir archivo", "Jumpseller"])

# Tab 1
with tab1:
    st.subheader("Pega aquí uno o varios contactos (separados por línea en blanco)")
    example = """Claro que si.
Nombre: Curaden Chile spa
Teléfono: 2222338466
Email: Marialexandra.perez@curaden.cl
Dirección: Callao 2970 oficina 206
Comuna: las condes.
Indicaciones: Dejar en conserjería
"""
    text = st.text_area("Texto de entrada", value=example, height=230)
    if st.button("Procesar texto", type="primary", key="btn_text"):
        if not text.strip():
            st.warning("Pega algún texto primero.")
        else:
            df = process_text(text, use_llm=usar_llm, contenido_paquete=contenido_paquete)
            st.success(f"{len(df)} registro(s) procesados. Edita antes de exportar.")
            edited = st.data_editor(df, num_rows="dynamic", use_container_width=True)
            xlsx = to_excel_template(edited)
            st.download_button("⬇️ Descargar Excel (LMT)", data=xlsx, file_name="Listado_de_Direcciones.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Tab 2
with tab2:
    st.subheader("Sube un archivo (.txt / .csv / .xlsx) con contactos")
    up = st.file_uploader("Archivo de entrada", type=["txt","csv","xlsx"])
    if up is not None:
        try:
            if up.type == "text/plain":
                content = up.read().decode("utf-8", errors="ignore")
                df = process_text(content, use_llm=False, contenido_paquete=contenido_paquete)
            elif up.name.endswith(".csv") or up.type == "text/csv":
                df_in = pd.read_csv(up)
                mapping = {
                    "nombre":"Nombre","first_name":"Nombre","nombres":"Nombre",
                    "apellido":"Apellido","last_name":"Apellido","apellidos":"Apellido",
                    "telefono":"Teléfono","tel":"Teléfono","movil":"Teléfono","celular":"Teléfono",
                    "direccion":"Dirección","address":"Dirección",
                    "comuna":"Comuna","ciudad":"Comuna",
                    "indicaciones":"Indicaciones","notas":"Indicaciones",
                    "id interno":"ID Interno","id":"ID Interno","orden":"ID Interno","po":"ID Interno","referencia":"ID Interno",
                    "correo":"Correo","email":"Correo","e-mail":"Correo",
                    "contenido del paquete":"Contenido del paquete","contenido":"Contenido del paquete",
                    "cantidad de bultos":"cantidad de bultos","bultos":"cantidad de bultos",
                }
                rows = []
                for _, row in df_in.iterrows():
                    d = {}
                    for c in df_in.columns:
                        key = re.sub(r"\s+"," ", str(c).strip().lower())
                        std = mapping.get(key)
                        if std:
                            d[std] = row[c]
                    base_dir, unit_note = _extract_unit_note(str(d.get("Dirección","")).strip())
                    d["Dirección"] = base_dir
                    if unit_note:
                        d["Indicaciones"] = (str(d.get("Indicaciones","")).strip() + ("; " if str(d.get("Indicaciones","")).strip() else "") + unit_note)
                    d["Teléfono"] = normalize_phone_cl(str(d.get("Teléfono","")))
                    d["Correo"] = validate_email_safe(str(d.get("Correo","")))
                    d["Comuna"] = match_comuna(str(d.get("Comuna","")))
                    d.setdefault("Contenido del paquete", contenido_paquete)
                    d.setdefault("cantidad de bultos", 1)
                    d.setdefault("ID Interno", _autogen_id_interno(d.get("Teléfono","")))
                    rows.append({col: d.get(col,"") for col in LMT_COLUMNS})
                df = pd.DataFrame(rows, columns=LMT_COLUMNS)
            else:
                df_in = pd.read_excel(up)
                # Reusar el mismo mapeo que CSV
                mapping = {
                    "nombre":"Nombre","first_name":"Nombre","nombres":"Nombre",
                    "apellido":"Apellido","last_name":"Apellido","apellidos":"Apellido",
                    "telefono":"Teléfono","tel":"Teléfono","movil":"Teléfono","celular":"Teléfono",
                    "direccion":"Dirección","address":"Dirección",
                    "comuna":"Comuna","ciudad":"Comuna",
                    "indicaciones":"Indicaciones","notas":"Indicaciones",
                    "id interno":"ID Interno","id":"ID Interno","orden":"ID Interno","po":"ID Interno","referencia":"ID Interno",
                    "correo":"Correo","email":"Correo","e-mail":"Correo",
                    "contenido del paquete":"Contenido del paquete","contenido":"Contenido del paquete",
                    "cantidad de bultos":"cantidad de bultos","bultos":"cantidad de bultos",
                }
                rows = []
                for _, row in df_in.iterrows():
                    d = {}
                    for c in df_in.columns:
                        key = re.sub(r"\s+"," ", str(c).strip().lower())
                        std = mapping.get(key)
                        if std:
                            d[std] = row[c]
                    base_dir, unit_note = _extract_unit_note(str(d.get("Dirección","")).strip())
                    d["Dirección"] = base_dir
                    if unit_note:
                        d["Indicaciones"] = (str(d.get("Indicaciones","")).strip() + ("; " if str(d.get("Indicaciones","")).strip() else "") + unit_note)
                    d["Teléfono"] = normalize_phone_cl(str(d.get("Teléfono","")))
                    d["Correo"] = validate_email_safe(str(d.get("Correo","")))
                    d["Comuna"] = match_comuna(str(d.get("Comuna","")))
                    d.setdefault("Contenido del paquete", contenido_paquete)
                    d.setdefault("cantidad de bultos", 1)
                    d.setdefault("ID Interno", _autogen_id_interno(d.get("Teléfono","")))
                    rows.append({col: d.get(col,"") for col in LMT_COLUMNS})
                df = pd.DataFrame(rows, columns=LMT_COLUMNS)

            st.success(f"{len(df)} registro(s) procesados. Edita antes de exportar.")
            edited = st.data_editor(df, num_rows="dynamic", use_container_width=True)
            xlsx = to_excel_template(edited)
            st.download_button("⬇️ Descargar Excel (LMT)", data=xlsx, file_name="Listado_de_Direcciones.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.error(f"Error procesando el archivo: {e}")

# Tab 3
with tab3:
    st.subheader("Importar pedidos desde Jumpseller (mecánico, sin IA)")
    colA, colB = st.columns(2)
    with colA:
        start = st.date_input("Fecha desde", value=date.today()-timedelta(days=7))
    with colB:
        end = st.date_input("Fecha hasta", value=date.today())
    status_options = ["paid","authorized","pending","shipped","cancelled"]
    statuses = st.multiselect("Estados a incluir", status_options, default=["paid"])

    if st.button("Cargar pedidos", type="primary", key="btn_jumpseller"):
        orders = fetch_jumpseller_orders(start, end, statuses)
        if not orders:
            st.warning("No se encontraron pedidos para ese rango/filtros.")
        else:
            df = map_jumpseller_to_rows(orders, contenido_paquete=contenido_paquete)
            st.success(f"Se cargaron {len(df)} pedido(s) de Jumpseller. Edita lo que necesites y exporta.")
            edited = st.data_editor(df, num_rows="dynamic", use_container_width=True)
            xlsx = to_excel_template(edited)
            st.download_button("⬇️ Descargar Excel (LMT)", data=xlsx, file_name="Listado_de_Direcciones.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
