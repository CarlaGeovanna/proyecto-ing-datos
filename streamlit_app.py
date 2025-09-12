
import io
import time
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Azure MySQL + CSV (Flexible, Metrics+Excel)", layout="wide")
st.title("📊 Azure MySQL + CSV Uploader (Flexible) — Métricas + Exportar a Excel")

# Fuente de datos
st.sidebar.header("Fuente de datos")
source = st.sidebar.radio("Selecciona la fuente", ["MySQL (Azure)", "CSV (subido)"])

# ===== MySQL helpers =====
def get_engine_from_secrets():
    cfg = st.secrets.get("mysql", {})
    host = cfg.get("host", "")
    port = int(cfg.get("port", 3306))
    user = cfg.get("user", "")
    password = cfg.get("password", "")
    database = cfg.get("database", "")
    if not all([host, user, password, database]):
        st.warning("""
        Secrets incompletos. Ve a **Settings → Secrets** y define:
        ```toml
        [mysql]
        host = "20.36.136.135"
        port = 3306
        user = "dash"
        password = "TU_PASS_FUERTE"
        database = "mydatabase"
        ```
        """)
        return None
    return create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}",
        pool_pre_ping=True
    )

@st.cache_data(ttl=300, show_spinner=False)
def load_from_mysql():
    engine = get_engine_from_secrets()
    if engine is None:
        return None
    with engine.begin() as con:
        df = pd.read_sql(
            text("""
                SELECT id, nombre, producto, precio, fecha
                FROM productos
            """),
            con,
            parse_dates=["fecha"],
        )
    return df

# ===== CSV helpers =====
def read_csv_safely(file):
    try:
        df = pd.read_csv(file)
    except Exception:
        file.seek(0)
        df = pd.read_csv(file, sep=';')
    return df

def normalize_datetime(series):
    try:
        return pd.to_datetime(series)
    except Exception:
        return series

def coerce_numeric(series):
    s = series.astype(str).str.replace(r"[^\d,.\-]", "", regex=True)
    if (s.str.count(",") > 0).sum() > (s.str.count(r"\.") > 0).sum():
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

# Subida CSV
uploaded_file = None
if source == "CSV (subido)":
    uploaded_file = st.sidebar.file_uploader("Subir CSV", type=["csv"])

# Botón actualizar
colA, colB = st.columns([1, 5])
with colA:
    if st.button("🔄 Actualizar"):
        load_from_mysql.clear()
        st.experimental_rerun()

# Carga de datos
df = None
if source == "MySQL (Azure)":
    with st.spinner("Conectando a MySQL en Azure..."):
        df = load_from_mysql()
        if df is None:
            st.stop()
elif source == "CSV (subido)":
    if uploaded_file is not None:
        with st.spinner("Leyendo CSV..."):
            df = read_csv_safely(uploaded_file)
    else:
        st.info("Sube un archivo CSV para continuar.")
        st.stop()

# Mapeo de columnas
st.sidebar.header("Mapeo de columnas (CSV)")
cols = list(df.columns)

def pick(label, default_candidates):
    found = None
    lower = {c.lower(): c for c in cols}
    for cand in default_candidates:
        if cand in lower:
            found = lower[cand]
            break
    return st.sidebar.selectbox(f"{label}:", ["(ninguna)"] + cols, index=(cols.index(found)+1) if found else 0)

if source == "CSV (subido)":
    c_id      = pick("ID (opcional)", ["id"])
    c_nombre  = pick("Nombre (opcional)", ["nombre", "cliente", "user", "buyer"])
    c_prod    = pick("Producto (requerido)", ["producto", "item", "product", "categoria"])
    c_precio  = pick("Precio (requerido)", ["precio", "amount", "price", "total"])
    c_fecha   = pick("Fecha (opcional)", ["fecha", "date", "timestamp", "datetime"])
else:
    c_id, c_nombre, c_prod, c_precio, c_fecha = "id", "nombre", "producto", "precio", "fecha"

if (c_prod == "(ninguna)") or (c_precio == "(ninguna)"):
    st.error("Debes mapear al menos **Producto** y **Precio**.")
    st.stop()

ndf = pd.DataFrame()
if c_id != "(ninguna)":
    ndf["id"] = df[c_id]
else:
    ndf["id"] = range(1, len(df) + 1)
if c_nombre != "(ninguna)":
    ndf["nombre"] = df[c_nombre]
else:
    ndf["nombre"] = None
ndf["producto"] = df[c_prod]
ndf["precio"] = coerce_numeric(df[c_precio])
if c_fecha != "(ninguna)":
    ndf["fecha"] = normalize_datetime(df[c_fecha])
else:
    ndf["fecha"] = pd.NaT

# ===== Panel de métricas =====
st.sidebar.header("Opciones de Métrica")
metric_choice = st.sidebar.selectbox("Métrica para Totales por producto", ["Suma (precio)", "Promedio (precio)", "Conteo (registros)"])

# Filtros
st.sidebar.header("Filtros")
prods = sorted(pd.Series(ndf["producto"]).dropna().astype(str).unique().tolist())
default_sel = prods[:5] if prods else []
prod_sel = st.sidebar.multiselect("Producto", prods, default=default_sel)

df_f = ndf.copy()
if prod_sel:
    df_f = df_f[df_f["producto"].astype(str).isin([str(x) for x in prod_sel])]

if df_f["fecha"].notna().any():
    min_date = pd.to_datetime(df_f["fecha"]).min()
    max_date = pd.to_datetime(df_f["fecha"]).max()
    if pd.notna(min_date) and pd.notna(max_date) and min_date != max_date:
        date_range = st.sidebar.date_input("Rango de fechas", (min_date.date(), max_date.date()))
        if isinstance(date_range, tuple) and len(date_range) == 2:
            d0, d1 = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
            df_f = df_f[(df_f["fecha"] >= d0) & (df_f["fecha"] <= d1 + pd.Timedelta(days=1))]

# KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric("Registros", f"{len(df_f):,}")
col2.metric("Total facturado", f"${df_f['precio'].sum():,.2f}")
col3.metric("Precio promedio", f"${df_f['precio'].mean():.2f}")
col4.metric("Precio máx.", f"${df_f['precio'].max():,.2f}")

# Totales por producto con selector de métrica
st.subheader("Totales por producto (según métrica)")
if metric_choice == "Suma (precio)":
    agg = df_f.groupby("producto", dropna=False).agg(valor=("precio", "sum"), n=("id", "count"))
    y_label = "Suma de precio"
elif metric_choice == "Promedio (precio)":
    agg = df_f.groupby("producto", dropna=False).agg(valor=("precio", "mean"), n=("id", "count"))
    y_label = "Precio promedio"
else:  # Conteo
    agg = df_f.groupby("producto", dropna=False).agg(valor=("id", "count"), n=("id", "count"))
    y_label = "Conteo"

agg = agg.sort_values("valor", ascending=False)
top_n = st.slider("Top N", min_value=3, max_value=30, value=10, step=1)
agg_top = agg.head(top_n)

c1, c2 = st.columns(2)
with c1:
    st.bar_chart(agg_top["valor"])
with c2:
    st.bar_chart(agg_top["n"])

# Series de tiempo
if df_f["fecha"].notna().any():
    st.subheader("Evolución temporal")
    df_t = df_f.copy()
    df_t["__day__"] = pd.to_datetime(df_t["fecha"]).dt.date
    by_day = df_t.groupby("__day__").agg(
        precio_promedio=("precio", "mean"),
        ventas=("id", "count"),
        facturado=("precio", "sum")
    )
    c3, c4 = st.columns(2)
    with c3:
        st.line_chart(by_day["facturado"])
    with c4:
        st.line_chart(by_day["ventas"])
else:
    st.info("No hay columna de fecha válida; se omiten gráficos de serie temporal.")

# Distribución
st.subheader("Distribución de precios")
st.bar_chart(df_f["precio"].dropna())

# Tabla + descargas
st.subheader("Tabla detallada (filtrada)")
st.dataframe(df_f.sort_values(by=["fecha"] if df_f["fecha"].notna().any() else ["id"],
                              ascending=False),
             use_container_width=True, height=420)

# Descarga CSV
csv_bytes = df_f.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Descargar filtrado (CSV)", data=csv_bytes, file_name="filtrado.csv", mime="text/csv")

# Descarga Excel (xlsx)
@st.cache_data
def to_excel_bytes(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="filtrado")
    return output.getvalue()

xlsx_bytes = to_excel_bytes(df_f)
st.download_button("⬇️ Descargar filtrado (Excel)", data=xlsx_bytes, file_name="filtrado.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.caption("Fuente: {}. Mapeo flexible para CSV. Botón 'Actualizar' limpia caché de MySQL y recarga.".format(source))
