import io
import re
import json
import time
from typing import Dict, Any, Optional

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
    "bultos": 1,
    "usar_llm": False,
    "umbral_coincidencia_comuna": 85,
}

# Comunas de Chile (lista resumida/representativa; puedes ampliar libremente)
COMUNAS_CHILE = [
    # Región Metropolitana
    "Cerrillos","Cerro Navia","Conchalí","El Bosque","Estación Central","Huechuraba",
    "Independencia","La Cisterna","La Florida","La Granja","La Pintana","La Reina",
    "Las Condes","Lo Barnechea","Lo Espejo","Lo Prado","Macul","Maipú","Ñuñoa",
    "Pedro Aguirre Cerda","Peñalolén","Providencia","Pudahuel","Quilicura","Quinta Normal",
    "Recoleta","Renca","San Joaquín","San Miguel","San Ramón","Santiago","Vitacura",
    # Valparaíso (selección)
    "Valparaíso","Viña del Mar","Quilpué","Villa Alemana","Quillota","Concón",
    # Biobío (selección)
    "Concepción","Talcahuano","San Pedro de la Paz","Coronel","Chiguayante",
    # Otras comunes conocidas
    "Antofagasta","Iquique","Arica","La Serena","Coquimbo","Rancagua","Talca",
    "Temuco","Valdivia","Puerto Montt","Punta Arenas"
]

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
    s = re.sub(r"[^\d+]", "", phone_raw)
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
    nombre = " ".join(parts[:-1])
    apellido = parts[-1]
    return nombre, apellido

def _extract_unit_note(direccion: str):
    """
    Separa 'depto/oficina' del final de la dirección.
    Devuelve (direccion_sin_unidad, nota_corta) donde nota_corta es por ejemplo 'Depto 1205' u 'Of. 206'.
    """
    if not direccion:
        return "", ""
    s = direccion.strip()

    pat = re.compile(r"""^(?P<base>.*?)(?:[,;\- ]+)?
                         (?:(?P<tag>depto|dpto|departamento|oficina|of\.?|of)\s*
                         (?P<num>[A-Za-z0-9\-]+))\s*$""", re.IGNORECASE | re.X)
    m = pat.match(s)
    if m:
        base = m.group('base').strip(' ,;-')
        tag = m.group('tag').lower().replace('departamento','depto')
        tag_norm = 'Depto' if tag in ('depto','dpto') else 'Of.'
        num = m.group('num')
        note = f"{tag_norm} {num}"
        return base, note

    return s, ""

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

def llm_parse_block(block: str) -> Optional[Dict[str, Any]]:
    if not st.secrets.get("openai_api_key"):
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=st.secrets["openai_api_key"])

        prompt = f"""
Eres un parser estricto. Extrae del texto los campos en JSON con estas claves:
- nombre_completo
- telefono
- correo
- direccion
- comuna
- indicaciones
- id_interno

Responde SOLO JSON válido sin comentarios.

Texto:
'''{block}'''
"""

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
        indic = (data.get("indicaciones","").strip() + ("; " if data.get("indicaciones","").strip() else "") + unit_note) if unit_note else data.get("indicaciones","").strip()

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

def process_text(text: str, use_llm: bool, contenido_paquete: str, bultos: int) -> pd.DataFrame:
    blocks = re.split(r"\n\s*\n|^-{3,}$", text.strip(), flags=re.MULTILINE)
    rows = []
    idx = 0
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        idx += 1
        data = None
        if use_llm:
            data = llm_parse_block(block)
        if not data:
            data = parse_block_rule_based(block)

        data["Teléfono"] = normalize_phone_cl(data.get("Teléfono",""))
        data["Correo"] = validate_email_safe(data.get("Correo",""))
        data["Comuna"] = match_comuna(data.get("Comuna",""))

        data["Contenido del paquete"] = contenido_paquete
        data["cantidad de bultos"] = bultos

        if not data.get("ID Interno"):
            import random, string
            letters = "".join(random.choice(string.ascii_letters) for _ in range(2))
            last4 = re.sub(r"\D", "", data.get("Teléfono",""))[-4:] or "0000"
            data["ID Interno"] = f"{letters}{last4}"

        row = {col: data.get(col, "") for col in LMT_COLUMNS}
        rows.append(row)

    df = pd.DataFrame(rows, columns=LMT_COLUMNS)
    return df

def to_excel_template(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Listado de Direcciones", index=False)
        writer.book["Listado de Direcciones"].sheet_properties.tabColor = "00BFA6"
    buffer.seek(0)
    return buffer.getvalue()

def download_button_xlsx(binary, filename: str, label: str):
    st.download_button(
        label=label,
        data=binary,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

# ============== UI ==============
st.title("📦 Koala Courier Export")
st.caption("Convierte texto desordenado en Excel listo para subir al courier (Plantilla LMT).")

with st.sidebar:
    st.header("Ajustes")
    contenido_paquete = st.text_input("Contenido del paquete", DEFAULTS["contenido_paquete"])
    bultos = st.number_input("Cantidad de bultos (por defecto)", min_value=1, max_value=99, value=DEFAULTS["bultos"], step=1)
    usar_llm = st.toggle("Usar IA (OpenAI) para parseo complejo", value=DEFAULTS["usar_llm"])
    st.session_state["umbral_coincidencia_comuna"] = st.slider("Umbral de coincidencia comuna", 50, 100, DEFAULTS["umbral_coincidencia_comuna"])
    st.divider()
    st.markdown("**Estado API OpenAI**")
    if st.secrets.get("openai_api_key"):
        st.success("API key detectada en secrets.")
    else:
        st.info("Sin API key en secrets. Funcionará el parser local.")

tab1, tab2 = st.tabs(["Pegar texto", "Subir archivo"])

with tab1:
    st.subheader("Pega aquí uno o varios contactos")
    example = """Claro que si.
Nombre: Curaden Chile spa
Teléfono: 2222338466
Email: Marialexandra.perez@curaden.cl
Dirección: Callao 2970 oficina 206
Comuna: las condes.
Indicaciones: Dejar en conserjería
"""
    text = st.text_area("Texto de entrada", value=example, height=260)
    if st.button("Procesar texto", type="primary"):
        if not text.strip():
            st.warning("Pega algún texto primero.")
        else:
            df = process_text(text, use_llm=usar_llm, contenido_paquete=contenido_paquete, bultos=bultos)
            st.success(f"{len(df)} registro(s) procesados.")
            st.dataframe(df, use_container_width=True)
            xlsx = to_excel_template(df)
            download_button_xlsx(xlsx, "Listado_de_Direcciones.xlsx", "⬇️ Descargar Excel (LMT)")

with tab2:
    st.subheader("Sube un archivo (.txt / .csv / .xlsx) con contactos")
    up = st.file_uploader("Archivo de entrada", type=["txt","csv","xlsx"])
    if up is not None:
        try:
            if up.type == "text/plain":
                content = up.read().decode("utf-8", errors="ignore")
                df = process_text(content, use_llm=usar_llm, contenido_paquete=contenido_paquete, bultos=bultos)
            elif up.type in ["text/csv", "application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"] or up.name.endswith(".csv"):
                if up.name.endswith(".csv") or up.type == "text/csv":
                    df_in = pd.read_csv(up)
                else:
                    df_in = pd.read_excel(up)

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
                cols_std = {}
                for c in df_in.columns:
                    key = re.sub(r"\s+"," ", str(c).strip().lower())
                    cols_std[c] = mapping.get(key, None)

                out_rows = []
                for _, row in df_in.iterrows():
                    data = {}
                    for orig, std in cols_std.items():
                        if std:
                            data[std] = row.get(orig, "")
                    # Normalizar dirección/unidad
                    base_dir, unit_note = _extract_unit_note(str(data.get("Dirección","")).strip())
                    data["Dirección"] = base_dir
                    if unit_note:
                        data["Indicaciones"] = (str(data.get("Indicaciones","")).strip() + ("; " if str(data.get("Indicaciones","")).strip() else "") + unit_note)

                    data.setdefault("Contenido del paquete", contenido_paquete)
                    data.setdefault("cantidad de bultos", bultos)
                    data.setdefault("Nombre","")
                    data.setdefault("Apellido","")
                    data["Teléfono"] = normalize_phone_cl(str(data.get("Teléfono","")))
                    data["Correo"] = validate_email_safe(str(data.get("Correo","")))
                    data["Comuna"] = match_comuna(str(data.get("Comuna","")))
                    if not data.get("ID Interno"):
                        import random, string
                        letters = "".join(random.choice(string.ascii_letters) for _ in range(2))
                        last4 = re.sub(r"\D", "", data.get("Teléfono",""))[-4:] or "0000"
                        data["ID Interno"] = f"{letters}{last4}"
                    out_rows.append({col: data.get(col, "") for col in LMT_COLUMNS})

                df = pd.DataFrame(out_rows, columns=LMT_COLUMNS)

            else:
                st.error("Tipo de archivo no soportado.")
                df = None

            if df is not None:
                st.success(f"{len(df)} registro(s) procesados desde archivo.")
                st.dataframe(df, use_container_width=True)
                xlsx = to_excel_template(df)
                download_button_xlsx(xlsx, "Listado_de_Direcciones.xlsx", "⬇️ Descargar Excel (LMT)")

        except Exception as e:
            st.error(f"Error procesando el archivo: {e}")
