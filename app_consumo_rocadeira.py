import sqlite3
from datetime import date
import io
import numpy as np
import pandas as pd
import streamlit as st

# Placeholder DB funcs
def load_df(): return pd.DataFrame()
def add_derivatives(df): return df
def load_modelos(): return pd.DataFrame()

st.set_page_config(page_title="InfraTech • Consumo de Gasolina")

tab_rel = st.tabs(["Relatórios"])[0]
with tab_rel:
    df = add_derivatives(load_df())
    if df.empty:
        st.info("Sem registros para analisar.")
    else:
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
        st.download_button("Baixar Excel — Real x Fabricante",
            x4.getvalue(),
            "real_vs_fabricante.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
