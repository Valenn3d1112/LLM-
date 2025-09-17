import io, re, json
from datetime import date, timedelta
from typing import Dict, Any, Optional, List

import streamlit as st
import pandas as pd
from rapidfuzz import process, fuzz
import phonenumbers
from email_validator import validate_email, EmailNotValidError

st.set_page_config(page_title="Koala Courier Export", page_icon="📦", layout="wide")

LMT_COLUMNS = [
    "Nombre","Apellido","Teléfono","Dirección","Comuna","Indicaciones",
    "ID Interno","Correo","Contenido del paquete","cantidad de bultos"
]
DEFAULTS = {"contenido_paquete":"Gorros clínicos Koala Scrubs","usar_llm":False,"umbral_coincidencia_comuna":85}
COMUNAS_CHILE = ["Cerrillos","Cerro Navia","Conchalí","El Bosque","Estación Central","Huechuraba",
"Independencia","La Cisterna","La Florida","La Granja","La Pintana","La Reina","Las Condes","Lo Barnechea",
"Lo Espejo","Lo Prado","Macul","Maipú","Ñuñoa","Pedro Aguirre Cerda","Peñalolén","Providencia","Pudahuel",
"Quilicura","Quinta Normal","Recoleta","Renca","San Joaquín","San Miguel","San Ramón","Santiago","Vitacura",
"Valparaíso","Viña del Mar","Quilpué","Villa Alemana","Quillota","Concón","Concepción","Talcahuano",
"San Pedro de la Paz","Coronel","Chiguayante","Antofagasta","Iquique","Arica","La Serena","Coquimbo",
"Rancagua","Talca","Temuco","Valdivia","Puerto Montt","Punta Arenas"]

# ========= utils =========
def _norm(s:str)->str:
    import unicodedata
    s2 = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return re.sub(r"\s+"," ", s2).strip().lower()

def match_comuna(raw:str, threshold:int=85)->str:
    if not raw: return ""
    norm = _norm(raw)
    mp = {c:_norm(c) for c in COMUNAS_CHILE}
    for k,v in mp.items():
        if v==norm: return k
    best = process.extractOne(norm, list(mp.values()), scorer=fuzz.WRatio)
    if best and best[1] >= threshold:
        idx = list(mp.values()).index(best[0])
        return list(mp.keys())[idx]
    return raw

def normalize_phone_cl(phone_raw:str)->str:
    if not phone_raw: return ""
    s = re.sub(r"[^\d+]","", str(phone_raw))
    try:
        num = phonenumbers.parse(s, "CL")
        if phonenumbers.is_possible_number(num) and phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
    except Exception: pass
    if s.startswith("56"):
        if not s.startswith("+"): s = "+"+s
    elif s.startswith("0"):
        s = "+56"+s.lstrip("0")
    elif not s.startswith("+"):
        s = "+56"+s
    return s

def validate_email_safe(email_raw:str)->str:
    if not email_raw: return ""
    try:
        v = validate_email(email_raw, check_deliverability=False)
        return v.email
    except EmailNotValidError:
        return email_raw

def split_name(full_name:str):
    if not full_name: return "",""
    parts = re.split(r"\s+", full_name.strip())
    if len(parts)==1: return parts[0],""
    return " ".join(parts[:-1]), parts[-1]

def _extract_unit_note(direccion:str):
    if not direccion: return "",""
    s = direccion.strip()
    m = re.match(r"^(?P<base>.*?)(?:[,;\- ]+)?(?:(?P<tag>depto|dpto|departamento|oficina|of\.?|of)\s*(?P<num>[A-Za-z0-9\-]+))\s*$", s, flags=re.IGNORECASE)
    if m:
        base = m.group("base").strip(" ,;-")
        tag = m.group("tag").lower().replace("departamento","depto")
        tag_norm = "Depto" if tag in ("depto","dpto") else "Of."
        return base, f"{tag_norm} {m.group('num')}"
    return s, ""

def to_excel_template(df: pd.DataFrame)->bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Listado de Direcciones", index=False)
        writer.book["Listado de Direcciones"].sheet_properties.tabColor = "00BFA6"
    buffer.seek(0)
    return buffer.getvalue()

def _autogen_id_interno(phone:str)->str:
    import random,string
    letters = "".join(random.choice(string.ascii_letters) for _ in range(2))
    last4 = re.sub(r"\D","", phone or "")[-4:] or "0000"
    return f"{letters}{last4}"

# ========= TEXT PARSER =========
def extract_by_labels(text:str)->Dict[str,str]:
    pats = {
        "nombre": r"(?:^|\n)\s*(?:nombre|razon\s*social)\s*:\s*(.+)",
        "telefono": r"(?:^|\n)\s*(?:telefono|tel|celular|m[oó]vil)\s*:\s*([^\n]+)",
        "correo": r"(?:^|\n)\s*(?:correo|email|e-?mail)\s*:\s*([^\n]+)",
        "direccion": r"(?:^|\n)\s*(?:direcci[oó]n)\s*:\s*(.+)",
        "comuna": r"(?:^|\n)\s*(?:comuna)\s*:\s*(.+)",
        "indicaciones": r"(?:^|\n)\s*(?:indicaciones|notas)\s*:\s*(.+)",
        "id_interno": r"(?:^|\n)\s*(?:id\s*interno|orden|po|referencia)\s*:\s*(.+)",
    }
    out = {}
    for k,pat in pats.items():
        m = re.search(pat, text, flags=re.IGNORECASE)
        out[k] = m.group(1).strip() if m else ""
    return out

def llm_parse_block(block:str)->Optional[Dict[str,Any]]:
    if not st.secrets.get("openai_api_key"): return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=st.secrets["openai_api_key"])
        prompt = (
            "Eres un parser estricto. Extrae del texto los campos en JSON con estas claves:\n"
            "- nombre_completo\n- telefono\n- correo\n- direccion\n- comuna\n- indicaciones\n- id_interno\n\n"
            "Responde SOLO JSON válido sin comentarios.\n\nTexto:\n'''"+block+"'''"
        )
        resp = client.chat.completions.create(
            model=st.secrets.get("openai_model","gpt-4o-mini"),
            messages=[{"role":"system","content":"Devuelve únicamente JSON válido. Sin explicaciones."},
                      {"role":"user","content":prompt}],
            temperature=0
        )
        data = json.loads(resp.choices[0].message.content.strip())
        nombre, apellido = split_name(data.get("nombre_completo",""))
        base_dir, unit_note = _extract_unit_note(data.get("direccion","").strip())
        indic = data.get("indicaciones","").strip()
        if unit_note: indic = (indic + ("; " if indic else "") + unit_note)
        return {"Nombre":nombre,"Apellido":apellido,"Teléfono":data.get("telefono",""),
                "Dirección":base_dir,"Comuna":data.get("comuna",""),"Indicaciones":indic,
                "ID Interno":data.get("id_interno",""),"Correo":data.get("correo","")}
    except Exception as e:
        st.warning(f"Parser LLM falló: {e}")
        return None

def parse_block_rule_based(block:str)->Dict[str,Any]:
    d = extract_by_labels(block)
    nombre, apellido = split_name(d.get("nombre",""))
    correo = validate_email_safe(d.get("correo",""))
    telefono = normalize_phone_cl(d.get("telefono",""))
    comuna = match_comuna(d.get("comuna",""), threshold=st.session_state.get("umbral_coincidencia_comuna", DEFAULTS["umbral_coincidencia_comuna"]))
    base_dir, unit_note = _extract_unit_note((d.get("direccion") or "").strip())
    indic = (d.get("indicaciones","").strip())
    if unit_note: indic = (indic + ("; " if indic else "") + unit_note)
    return {"Nombre":nombre,"Apellido":apellido,"Teléfono":telefono,"Dirección":base_dir,
            "Comuna":comuna,"Indicaciones":indic,"ID Interno":d.get("id_interno","").strip(),"Correo":correo}

def process_text(text:str, use_llm:bool, contenido:str)->pd.DataFrame:
    blocks = re.split(r"\n\s*\n|^-{3,}$", text.strip(), flags=re.MULTILINE)
    rows = []
    for b in [b for b in blocks if b.strip()]:
        data = llm_parse_block(b) if use_llm else parse_block_rule_based(b)
        data["Teléfono"] = normalize_phone_cl(data.get("Teléfono",""))
        data["Correo"] = validate_email_safe(data.get("Correo",""))
        data["Comuna"] = match_comuna(data.get("Comuna",""))
        data["Contenido del paquete"] = contenido
        data["cantidad de bultos"] = 1
        if not data.get("ID Interno"): data["ID Interno"]=_autogen_id_interno(data.get("Teléfono",""))
        rows.append({col: data.get(col,"") for col in LMT_COLUMNS})
    return pd.DataFrame(rows, columns=LMT_COLUMNS)

# ========= JUMPSELLER =========
def fetch_jumpseller_orders(start:date, end:date, statuses:List[str], page_limit:int=100)->List[Dict[str,Any]]:
    login = st.secrets.get("jumpseller_login") or st.secrets.get("jumpseller_api_login")
    token = st.secrets.get("jumpseller_authtoken") or st.secrets.get("jumpseller_api_token")
    if not (login and token):
        st.error("Faltan secrets Jumpseller: jumpseller_login y jumpseller_authtoken")
        return []
    import requests
    base = "https://api.jumpseller.com/v1"
    orders = []
    def req(path, params):
        url = f"{base}{path}"
        q = dict(params or {})
        q["login"] = login
        q["authtoken"] = token
        return requests.get(url, params=q, timeout=30)
    if statuses and len(statuses)==1:
        status = statuses[0]
        page=1
        while True:
            r = req(f"/orders/status/{status}.json", {
                "created_at_min": start.isoformat(),
                "created_at_max": (end + timedelta(days=1)).isoformat(),
                "page": page, "limit": page_limit
            })
            if r.status_code!=200:
                st.error(f"Error Jumpseller {r.status_code}: {r.text[:300]}"); break
            items = r.json()
            items = items if isinstance(items,list) else items.get("orders",[])
            if not items: break
            orders.extend(items)
            if len(items) < page_limit: break
            page += 1
            if page>100: break
    else:
        page=1
        while True:
            r = req("/orders.json", {
                "created_at_min": start.isoformat(),
                "created_at_max": (end + timedelta(days=1)).isoformat(),
                "page": page, "limit": page_limit
            })
            if r.status_code!=200:
                st.error(f"Error Jumpseller {r.status_code}: {r.text[:300]}"); break
            items = r.json()
            items = items if isinstance(items,list) else items.get("orders",[])
            if not items: break
            if statuses:
                ok = set(statuses)
                items = [o for o in items if str(o.get("status","")).lower() in ok]
            orders.extend(items)
            if len(items) < page_limit: break
            page += 1
            if page>100: break
    return orders

def map_jumpseller_to_rows(orders:List[Dict[str,Any]], contenido_paquete:str)->pd.DataFrame:
    rows=[]
    for o in orders:
        shipping = o.get("shipping_address",{}) or {}
        customer = o.get("customer",{}) or {}

        full_name = shipping.get("name") or customer.get("name") or ""
        nombre, apellido = split_name(full_name)
        phone = shipping.get("phone") or customer.get("phone") or ""
        email = customer.get("email") or shipping.get("email") or ""

        direccion_raw = " ".join([p for p in [shipping.get("address"), shipping.get("address_2")] if p]) or ""
        base_dir, unit_note = _extract_unit_note(direccion_raw)

        comuna = shipping.get("city") or shipping.get("province") or ""
        comuna = match_comuna(comuna)

        indic = unit_note
        ref = shipping.get("reference") or o.get("note") or ""
        if ref: indic = (indic + ("; " if indic else "") + str(ref).strip())

        telefono_fmt = normalize_phone_cl(phone)
        correo_fmt = validate_email_safe(email)

        row = {"Nombre":nombre,"Apellido":apellido,"Teléfono":telefono_fmt,"Dirección":base_dir,
               "Comuna":comuna,"Indicaciones":indic,"ID Interno":_autogen_id_interno(telefono_fmt),
               "Correo":correo_fmt,"Contenido del paquete":contenido_paquete,"cantidad de bultos":1}
        rows.append({col:row.get(col,"") for col in LMT_COLUMNS})
    return pd.DataFrame(rows, columns=LMT_COLUMNS)

# ========= UI =========
st.title("📦 Koala Courier Export")
st.caption("Convierte TEXTO o pedidos de Jumpseller en Excel listo para el courier (Plantilla LMT).")

if "lote" not in st.session_state:
    st.session_state["lote"] = pd.DataFrame(columns=LMT_COLUMNS)

with st.sidebar:
    st.header("Ajustes")
    contenido_paquete = st.text_input("Contenido del paquete", DEFAULTS["contenido_paquete"])
    usar_llm = st.toggle("Usar IA (OpenAI) para parseo de TEXTO", value=DEFAULTS["usar_llm"])
    st.session_state["umbral_coincidencia_comuna"] = st.slider("Umbral de coincidencia comuna", 50, 100, DEFAULTS["umbral_coincidencia_comuna"])
    st.divider()
    st.markdown("**Estado API**")
    st.write("- OpenAI:", "✅ Detectada" if st.secrets.get("openai_api_key") else "—")
    jumpseller_ok = all([st.secrets.get(k) for k in ("jumpseller_login","jumpseller_authtoken")])
    st.write("- Jumpseller:", "✅ Listo" if jumpseller_ok else "—")
    st.divider()
    st.subheader("🧺 Lote actual")
    if len(st.session_state["lote"]):
        st.caption(f"{len(st.session_state['lote'])} filas acumuladas")
        if st.button("🗑️ Vaciar lote"):
            st.session_state["lote"] = pd.DataFrame(columns=LMT_COLUMNS)

tab1, tab2, tab3 = st.tabs(["Pegar texto (IA)", "Subir archivo", "Jumpseller"])

# TAB 1
with tab1:
    st.subheader("Pega aquí uno o varios contactos")
    example = """Nombre: Curaden Chile spa
Teléfono: 2222338466
Email: Marialexandra.perez@curaden.cl
Dirección: Callao 2970 oficina 206
Comuna: las condes.
Indicaciones: Dejar en conserjería"""
    text = st.text_area("Texto de entrada", value=example, height=230)
    if st.button("Procesar texto", type="primary", key="btn_text"):
        if not text.strip():
            st.warning("Pega algún texto primero.")
        else:
            df = process_text(text, use_llm=usar_llm, contenido=contenido_paquete)
            st.session_state["tmp_text"] = df
    if "tmp_text" in st.session_state:
        edited = st.data_editor(st.session_state["tmp_text"], num_rows="dynamic", use_container_width=True, key="edit_text")
        st.session_state["tmp_text"] = edited
        if st.button("➕ Agregar al lote", key="add_text"):
            st.session_state["lote"] = pd.concat([st.session_state["lote"], edited], ignore_index=True)
            del st.session_state["tmp_text"]
            st.success("Agregado al lote.")

# TAB 2
with tab2:
    st.subheader("Sube un archivo (.txt / .csv / .xlsx)")
    up = st.file_uploader("Archivo", type=["txt","csv","xlsx"])
    if up is not None:
        try:
            if up.type == "text/plain":
                content = up.read().decode("utf-8", errors="ignore")
                df = process_text(content, use_llm=False, contenido=contenido_paquete)
            elif up.name.endswith(".csv") or up.type == "text/csv":
                df = pd.read_csv(up)
            else:
                df = pd.read_excel(up)
            st.session_state["tmp_file"] = df
        except Exception as e:
            st.error(f"Error: {e}")
    if "tmp_file" in st.session_state:
        edited = st.data_editor(st.session_state["tmp_file"], num_rows="dynamic", use_container_width=True, key="edit_file")
        st.session_state["tmp_file"] = edited
        if st.button("➕ Agregar al lote", key="add_file"):
            st.session_state["lote"] = pd.concat([st.session_state["lote"], edited], ignore_index=True)
           
