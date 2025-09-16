# Koala Courier Export (MVP)

Convierte texto libre (copiar/pegar) o archivos (.txt/.csv/.xlsx) en un **Excel** con el formato exacto de **"Listado de Direcciones"** para tu courier.

## 🚀 Cómo usar
1. Crea un entorno (opcional) e instala dependencias:
   ```bash
   pip install -r requirements.txt
   ```
2. Añade tu API key de OpenAI en los **secrets de Streamlit** (solo si activarás el modo IA):
   ```toml
   # ~/.streamlit/secrets.toml
   openai_api_key = "sk-..."
   # opcional: elegir modelo
   openai_model = "gpt-4o-mini"
   ```
3. Ejecuta la app:
   ```bash
   streamlit run app.py
   ```
4. En la app:
   - Pega texto (1 o varios contactos) o sube un archivo.
   - Ajusta defaults en la barra lateral (contenido del paquete, bultos, etc.).
   - (Opcional) Activa **Usar IA** para parseos más complejos.
   - Descarga el Excel con el botón **"⬇️ Descargar Excel (LMT)"**.

## 🧠 Notas
- El parser local funciona **offline** (sin enviar datos a terceros).
- El modo IA usa tu API key y solo se invoca si lo activas.
- Se normalizan **teléfono** (Chile), **email** y **comuna** (con “fuzzy match”).

## 🧩 Columnas generadas
`Nombre | Apellido | Teléfono | Dirección | Comuna | Indicaciones | ID Interno | Correo | Contenido del paquete | cantidad de bultos`

---

Hecho para Koala Scrubs ❤️.