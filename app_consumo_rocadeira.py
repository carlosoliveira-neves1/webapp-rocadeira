import sqlite3
from datetime import date, datetime
from typing import Optional, Tuple, List
import io
import numpy as np

import pandas as pd
import streamlit as st

DB_PATH = "rocadeira.db"
M2_PER_HA = 10_000.0

st.set_page_config(page_title="InfraTech • Consumo de Gasolina", page_icon="⛽", layout="centered", initial_sidebar_state="collapsed")
st.markdown("""
<style>
.block-container {padding-top: 1rem; padding-bottom: 2rem;}
button[kind="primary"], .stDownloadButton button {padding: 0.8rem 1rem; border-radius: 12px; font-weight: 700;}
input, textarea, select {font-size: 1rem;}
.kpi {padding: 12px; border: 1px solid #e8e8e8; border-radius: 14px; margin-bottom: 8px}
.brand {display:flex; align-items:center; gap:10px}
.brand h1 {font-size: 1.25rem; margin: 0}
.brand-sub {color:#5f6c7b; font-size: 0.95rem}
@media (min-width: 900px) {.brand h1 {font-size: 1.6rem}}
</style>
""", unsafe_allow_html=True)

# ---------------- DB ---------------- #
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS abastecimentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                marca TEXT,
                modelo TEXT,
                equipamento TEXT DEFAULT 'Roçadeira',
                litros REAL NOT NULL,
                horas REAL NOT NULL,
                area_valor REAL NOT NULL,
                area_unidade TEXT CHECK(area_unidade IN ('m2','ha')) NOT NULL,
                preco_por_litro REAL,
                custo_total REAL,
                observacoes TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS modelos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                marca TEXT NOT NULL,
                modelo TEXT NOT NULL,
                consumo_fabricante_l_h REAL,
                UNIQUE(marca, modelo)
            )
            """
        )

def count_rows(table: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cur.fetchone()[0])

def seed_from_assets():
    """Carrega seeds XLSX se o banco estiver vazio."""
    try:
        if count_rows("modelos") == 0:
            dfc = pd.read_excel("assets/seed_catalogo.xlsx")
            with sqlite3.connect(DB_PATH) as conn:
                for _, r in dfc.iterrows():
                    conn.execute("""
                        INSERT OR IGNORE INTO modelos(marca, modelo, consumo_fabricante_l_h)
                        VALUES (?,?,?)
                    """, (r["marca"], r["modelo"], float(r["consumo_fabricante_l_h"]) if pd.notna(r["consumo_fabricante_l_h"]) else None))
        if count_rows("abastecimentos") == 0:
            dfa = pd.read_excel("assets/seed_abastecimentos.xlsx", sheet_name="abastecimentos")
            with sqlite3.connect(DB_PATH) as conn:
                for _, r in dfa.iterrows():
                    conn.execute("""
                        INSERT INTO abastecimentos
                        (data, marca, modelo, equipamento, litros, horas, area_valor, area_unidade, preco_por_litro, custo_total, observacoes)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (str(r["data"])[:10], r["marca"], r["modelo"], r.get("equipamento","Roçadeira"),
                          float(r["litros"]), float(r["horas"]), float(r["area_valor"]), r["area_unidade"],
                          float(r["preco_por_litro"]) if pd.notna(r["preco_por_litro"]) else None,
                          float(r["custo_total"]) if pd.notna(r["custo_total"]) else None,
                          str(r.get("observacoes",""))))
    except Exception as ex:
        st.warning(f"Seed automático não carregado: {ex}")

def insert_row(data: date, marca: str, modelo: str, equipamento: str, litros: float, horas: float,
               area_valor: float, area_unidade: str, preco_por_litro: Optional[float],
               custo_total: Optional[float], observacoes: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO abastecimentos
            (data, marca, modelo, equipamento, litros, horas, area_valor, area_unidade, preco_por_litro, custo_total, observacoes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (data.isoformat(), marca, modelo, equipamento, litros, horas, area_valor, area_unidade, preco_por_litro, custo_total, observacoes)
        )

def load_df() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM abastecimentos ORDER BY date(data) DESC, id DESC", conn)
    if not df.empty: df["data"] = pd.to_datetime(df["data"]).dt.date
    return df

def load_modelos() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM modelos ORDER BY marca, modelo", conn)
    return df

def upsert_modelo(marca: str, modelo: str, consumo: Optional[float]):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO modelos(marca, modelo, consumo_fabricante_l_h)
            VALUES (?,?,?)
            ON CONFLICT(marca, modelo) DO UPDATE SET consumo_fabricante_l_h=excluded.consumo_fabricante_l_h
            """,
            (marca, modelo, consumo)
        )

# ------------- Utils ------------- #
def to_m2(area_valor: float, area_unidade: str) -> float:
    return area_valor * (M2_PER_HA if area_unidade == "ha" else 1.0)

def compute_costs(litros: float, preco_por_litro: Optional[float], custo_total: Optional[float]):
    if preco_por_litro is None and custo_total is not None and litros>0:
        preco_por_litro = custo_total / litros
    if custo_total is None and preco_por_litro is not None:
        custo_total = preco_por_litro * litros
    return preco_por_litro, custo_total

def add_derivatives(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df = df.copy()
    df["area_m2"] = df.apply(lambda r: to_m2(r["area_valor"], r["area_unidade"]), axis=1)
    df["L/h"]  = (df["litros"] / df["horas"]).replace([pd.NA, pd.NaT], pd.NA)
    df["L/m²"] = (df["litros"] / df["area_m2"]).replace([pd.NA, pd.NaT], pd.NA)
    df["L/ha"] = (df["litros"] / (df["area_m2"]/M2_PER_HA)).replace([pd.NA, pd.NaT], pd.NA)
    df[["preco_por_litro","custo_total"]] = df.apply(
        lambda r: pd.Series(compute_costs(r["litros"], r["preco_por_litro"], r["custo_total"])), axis=1
    )
    df["Custo/h"]  = (df["custo_total"] / df["horas"]).replace([pd.NA, pd.NaT], pd.NA)
    df["Custo/ha"] = (df["custo_total"] / (df["area_m2"]/M2_PER_HA)).replace([pd.NA, pd.NaT], pd.NA)
    return df

def month_str(d: date) -> str:
    return pd.to_datetime(d).strftime("%Y-%m")

# ------------- Header ------------- #
st.image("assets/logo.png", width=90)
st.markdown('<div class="brand"><h1>InfraTech — Consumo de Gasolina</h1></div>', unsafe_allow_html=True)
st.markdown('<div class="brand-sub">Aplicativo desenvolvido por <b>INFRATECH • Dados e Elétrica</b>. Soluções em dados e elétrica com excelência.</div>', unsafe_allow_html=True)

# Init + seed automático (se banco vazio)
init_db()
seed_from_assets()

# ------------- Tabs ------------- #
tab_reg, tab_dash, tab_hist, tab_rel, tab_cat, tab_imp = st.tabs(["Registrar", "Dashboard", "Histórico", "Relatórios", "Catálogo", "Importar"])

# -------- Registrar -------- #
with tab_reg:
    st.subheader("Novo abastecimento")
    modelos_df = load_modelos()
    marcas = sorted(modelos_df["marca"].unique().tolist())
    marca = st.selectbox("Marca da roçadeira", options=marcas if marcas else ["—"], key="marca_reg")
    modelos = modelos_df.loc[modelos_df["marca"]==marca, "modelo"].tolist() if marca != "—" else []
    modelo = st.selectbox("Modelo da roçadeira", options=modelos if modelos else ["—"], key="modelo_reg")

    # consumo fabricante (se existir)
    consumo_info = modelos_df[(modelos_df["marca"]==marca) & (modelos_df["modelo"]==modelo)]
    consumo_fab = consumo_info["consumo_fabricante_l_h"].iloc[0] if not consumo_info.empty else None
    if pd.notna(consumo_fab) and consumo_fab > 0:
        st.success(f"Consumo médio do fabricante: **{consumo_fab:.2f} L/h**")
    else:
        st.info("Consumo do fabricante não cadastrado. Você pode cadastrar na aba **Catálogo**.")

    with st.form("form_abast", clear_on_submit=True):
        data = st.date_input("Data do abastecimento", value=date.today(), key="data_reg")
        equipamento = st.text_input("Identificação do equipamento (opcional)", value="Roçadeira", key="equip_reg")
        litros = st.number_input("Litros abastecidos", min_value=0.0, step=0.1, format="%.2f", key="litros_reg")
        horas  = st.number_input("Horas trabalhadas desde o abastecimento", min_value=0.0, step=0.1, format="%.2f", key="horas_reg")
        area_valor = st.number_input("Área roçada", min_value=0.0, step=1.0, format="%.2f", key="area_reg")
        area_unidade = st.selectbox("Unidade da área", ["m2","ha"], index=0, key="area_unid_reg")
        preco_por_litro = st.number_input("Preço por litro (R$) – opcional", min_value=0.0, step=0.1, format="%.3f", key="ppl_reg")
        custo_total = st.number_input("Custo total (R$) – opcional", min_value=0.0, step=0.1, format="%.2f", key="ct_reg")
        observacoes = st.text_area("Observações (opcional)", key="obs_reg")
        if preco_por_litro == 0: preco_por_litro = None
        if custo_total == 0: custo_total = None
        submitted = st.form_submit_button("Salvar abastecimento")
        if submitted:
            if marca=="—" or modelo=="—" or litros<=0 or horas<=0 or area_valor<=0:
                st.error("Selecione marca/modelo e preencha litros, horas e área (> 0).")
            else:
                ppl, ct = compute_costs(litros, preco_por_litro, custo_total)
                insert_row(data, marca, modelo, equipamento, litros, horas, area_valor, area_unidade, ppl, ct, observacoes)
                st.success("Abastecimento registrado!")

# -------- Dashboard -------- #
with tab_dash:
    df = add_derivatives(load_df())
    if df.empty:
        st.info("Sem registros ainda.")
    else:
        modelos_df = load_modelos()
        marcas = ["Todas"] + sorted(modelos_df["marca"].unique().tolist())
        marca_f = st.selectbox("Filtrar por marca", options=marcas, key="marca_dash")
        if marca_f != "Todas":
            modelos = ["Todos"] + modelos_df.loc[modelos_df["marca"]==marca_f, "modelo"].tolist()
        else:
            modelos = ["Todos"]
        modelo_f = st.selectbox("Filtrar por modelo", options=modelos, key="modelo_dash")

        mask = pd.Series(True, index=df.index)
        if marca_f != "Todas": mask &= (df["marca"]==marca_f)
        if modelo_f != "Todos": mask &= (df["modelo"]==modelo_f)
        dfx = df.loc[mask].copy()

        k1,k2,k3,k4,k5 = st.columns(5)
        with k1: st.markdown('<div class="kpi">Média L/h<br><b>{}</b></div>'.format(f"{dfx['L/h'].mean():.2f}" if dfx["L/h"].notna().any() else "–"), unsafe_allow_html=True)
        with k2: st.markdown('<div class="kpi">Média L/ha<br><b>{}</b></div>'.format(f"{dfx['L/ha'].mean():.2f}" if dfx["L/ha"].notna().any() else "–"), unsafe_allow_html=True)
        with k3: st.markdown('<div class="kpi">Litros<br><b>{:.2f}</b></div>'.format(dfx["litros"].sum()), unsafe_allow_html=True)
        with k4: st.markdown('<div class="kpi">Custo (R$)<br><b>{}</b></div>'.format(f"{dfx['custo_total'].sum():.2f}" if dfx["custo_total"].notna().any() else "–"), unsafe_allow_html=True)
        with k5: 
            preco_med = (dfx["custo_total"].sum()/dfx["litros"].sum()) if dfx["custo_total"].notna().any() and dfx["litros"].sum()>0 else None
            st.markdown('<div class="kpi">R$/L<br><b>{}</b></div>'.format(f"{preco_med:.2f}" if preco_med else "–"), unsafe_allow_html=True)

        st.markdown("##### Tendência L/h")
        if not dfx.empty:
            st.line_chart(dfx.sort_values("data").set_index("data")["L/h"], height=220)

# -------- Histórico -------- #
with tab_hist:
    df = add_derivatives(load_df())
    if df.empty:
        st.info("Sem registros.")
    else:
        modelos_df = load_modelos()
        marcas = ["Todas"] + sorted(modelos_df["marca"].unique().tolist())
        marca_f = st.selectbox("Filtrar por marca", options=marcas, key="marca_hist")
        if marca_f != "Todas":
            modelos = ["Todos"] + modelos_df.loc[modelos_df["marca"]==marca_f, "modelo"].tolist()
        else:
            modelos = ["Todos"]
        modelo_f = st.selectbox("Filtrar por modelo", options=modelos, key="modelo_hist")

        texto = st.text_input("Buscar em observações/identificação", key="busca_hist")
        mask = pd.Series(True, index=df.index)
        if marca_f != "Todas": mask &= (df["marca"]==marca_f)
        if modelo_f != "Todos": mask &= (df["modelo"]==modelo_f)
        if texto:
            mask &= (df["observacoes"].fillna("").str.contains(texto, case=False) | df["equipamento"].fillna("").str.contains(texto, case=False))
        dfx = df.loc[mask]
        show_cols = ["data","marca","modelo","equipamento","litros","horas","area_valor","area_unidade","preco_por_litro","custo_total","L/h","L/ha","observacoes"]
        st.dataframe(dfx[show_cols], use_container_width=True, hide_index=True)
        # Exportar Excel XLSX
        xbio = io.BytesIO()
        with pd.ExcelWriter(xbio, engine="openpyxl") as writer:
            dfx.to_excel(writer, index=False, sheet_name="Historico")
        st.download_button("Baixar Excel (XLSX)", xbio.getvalue(), "historico_consumo_rocadeira.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# -------- Relatórios -------- #
with tab_rel:
    df = add_derivatives(load_df())
    if df.empty:
        st.info("Sem registros para analisar.")
    else:
        st.markdown("### 1) Consolidado Mensal")
        dff = df.copy()
        dff["mes"] = dff["data"].map(month_str)
        agg = dff.groupby("mes").agg(
            litros_total=("litros","sum"),
            horas_total=("horas","sum"),
            area_total_m2=("area_m2","sum"),
            custo_total=("custo_total","sum"),
        ).reset_index()
        agg["L/h_médio"] = dff.groupby("mes")["L/h"].mean().values
        agg["L/ha_médio"] = dff.groupby("mes")["L/ha"].mean().values
        st.dataframe(agg, use_container_width=True)
        x1 = io.BytesIO()
        with pd.ExcelWriter(x1, engine="openpyxl") as w:
            agg.to_excel(w, index=False, sheet_name="Consolidado Mensal")
        st.download_button("Baixar Excel — Consolidado Mensal", x1.getvalue(), "consolidado_mensal.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.markdown("### 2) Por Equipamento (Marca/Modelo)")
        ge = df.groupby(["marca","modelo"]).agg(
            abastecimentos=("id","count"),
            litros=("litros","sum"),
            horas=("horas","sum"),
            area_m2=("area_m2","sum"),
            custo_total=("custo_total","sum"),
            L_h_medio=("L/h","mean"),
            L_ha_medio=("L/ha","mean"),
        ).reset_index()
        st.dataframe(ge, use_container_width=True)
        x2 = io.BytesIO()
        with pd.ExcelWriter(x2, engine="openpyxl") as w:
            ge.to_excel(w, index=False, sheet_name="Por Equipamento")
        st.download_button("Baixar Excel — Por Equipamento", x2.getvalue(), "por_equipamento.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.markdown("### 3) Ranking por Eficiência (menor L/ha)")
        rank = df.sort_values("L/ha").loc[:, ["data","marca","modelo","equipamento","litros","horas","area_unidade","area_valor","L/ha","L/h","custo_total","observacoes"]].head(50)
        st.dataframe(rank, use_container_width=True)
        x3 = io.BytesIO()
        with pd.ExcelWriter(x3, engine="openpyxl") as w:
            rank.to_excel(w, index=False, sheet_name="Ranking Eficiencia")
        st.download_button("Baixar Excel — Ranking por Eficiência", x3.getvalue(), "ranking_eficiencia.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.markdown("### 4) Consumo Real × Fabricante (comparativo)")
        modelos_df = load_modelos()
        dfm = df.merge(modelos_df, on=["marca","modelo"], how="left")
        comp = dfm.groupby(["marca","modelo"]).agg(
            real_Lh=("L/h","mean"),
            fab_Lh=("consumo_fabricante_l_h","first")
        ).reset_index()

        comp["real_Lh"] = pd.to_numeric(comp["real_Lh"], errors="coerce")
        comp["fab_Lh"]  = pd.to_numeric(comp["fab_Lh"], errors="coerce")

        with np.errstate(divide='ignore', invalid='ignore'):
            dif = (comp["real_Lh"] - comp["fab_Lh"]) / comp["fab_Lh"]
            dif = np.where((comp["fab_Lh"] > 0) & np.isfinite(dif), dif * 100.0, np.nan)
        comp["dif_%"] = np.round(dif, 1)

        st.dataframe(comp, use_container_width=True)
        x4 = io.BytesIO()
        with pd.ExcelWriter(x4, engine="openpyxl") as w:
            comp.to_excel(w, index=False, sheet_name="Real_vs_Fabricante")
        st.download_button("Baixar Excel — Real x Fabricante", x4.getvalue(), "real_vs_fabricante.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# -------- Catálogo -------- #
with tab_cat:
    st.subheader("Catálogo de Modelos — Consumo do Fabricante (L/h)")
    modelos_df = load_modelos()
    st.dataframe(modelos_df, use_container_width=True, hide_index=True)
    st.markdown("---")
    st.markdown("**Atualizar/Adicionar consumo do fabricante**")
    if not modelos_df.empty:
        marca_e = st.selectbox("Marca da roçadeira", sorted(modelos_df["marca"].unique().tolist()), key="marca_edit")
        modelo_e = st.selectbox("Modelo da roçadeira", modelos_df.loc[modelos_df["marca"]==marca_e, "modelo"].tolist(), key="modelo_edit")
        consumo_e = st.number_input("Consumo oficial do fabricante (L/h)", min_value=0.0, step=0.1, format="%.2f", key="consumo_edit")
        if st.button("Salvar consumo do fabricante", type="primary", key="btn_edit"):
            upsert_modelo(marca_e, modelo_e, float(consumo_e) if consumo_e>0 else None)
            st.success("Consumo atualizado.")

# -------- Importar (XLSX) -------- #
with tab_imp:
    st.subheader("Importar dados de planilha (XLSX)")
    st.caption("Modelos de exemplo estão na pasta **assets/** do projeto: `seed_catalogo.xlsx` e `seed_abastecimentos.xlsx`.")
    up1 = st.file_uploader("Importar Catálogo de Modelos (marca, modelo, consumo_fabricante_l_h)", type=["xlsx"], key="up_cat")
    if up1 is not None:
        try:
            dfu = pd.read_excel(up1)
            ok = 0
            with sqlite3.connect(DB_PATH) as conn:
                for _, r in dfu.iterrows():
                    conn.execute("""
                        INSERT INTO modelos(marca, modelo, consumo_fabricante_l_h)
                        VALUES (?,?,?)
                        ON CONFLICT(marca, modelo) DO UPDATE SET consumo_fabricante_l_h=excluded.consumo_fabricante_l_h
                    """, (r["marca"], r["modelo"], float(r["consumo_fabricante_l_h"]) if pd.notna(r["consumo_fabricante_l_h"]) else None))
                    ok += 1
            st.success(f"Catálogo importado/atualizado: {ok} linhas.")
        except Exception as ex:
            st.error(f"Falha ao importar catálogo: {ex}")

    st.markdown("---")
    up2 = st.file_uploader("Importar Abastecimentos", type=["xlsx"], key="up_abast")
    if up2 is not None:
        try:
            dfu = pd.read_excel(up2)
            ok = 0
            with sqlite3.connect(DB_PATH) as conn:
                for _, r in dfu.iterrows():
                    conn.execute("""
                        INSERT INTO abastecimentos
                        (data, marca, modelo, equipamento, litros, horas, area_valor, area_unidade, preco_por_litro, custo_total, observacoes)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (str(r["data"])[:10], r["marca"], r["modelo"], r.get("equipamento","Roçadeira"),
                          float(r["litros"]), float(r["horas"]), float(r["area_valor"]), r["area_unidade"],
                          float(r["preco_por_litro"]) if pd.notna(r["preco_por_litro"]) else None,
                          float(r["custo_total"]) if pd.notna(r["custo_total"]) else None,
                          str(r.get("observacoes",""))))
                    ok += 1
            st.success(f"Abastecimentos importados: {ok} linhas.")
        except Exception as ex:
            st.error(f"Falha ao importar abastecimentos: {ex}")
