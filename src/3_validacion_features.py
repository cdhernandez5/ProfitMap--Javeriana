#!/usr/bin/env python3
"""
=============================================================================
SEC DERA — Pipeline Completo: Validación, Feature Engineering y Preparación
=============================================================================

Script unificado que ejecuta los pasos 3, 4 y 5 del pipeline de riesgo
financiero SEC DERA en una sola ejecución, sin CSVs intermedios.

  FASE 1 — Validación y Limpieza (9 pasos)
      Eliminar anomalías, deduplicar, corregir valores imposibles,
      limpiar qtrs anómalos y filtrar por rango temporal.

  FASE 2 — Feature Engineering (7 bloques, ~73 variables)
      Variables contextuales, anualizadas, ratios financieros,
      Z-Score de Altman, flags de alerta, métricas de acciones
      y cambios temporales.

  FASE 3 — Preparación del Dataset (5 sub-pasos)
      Eliminar columnas auxiliares (_qtrs), filtrar por cobertura,
      filtros de calidad, winsorización de outliers y reporte final.

Entrada:  CSV crudo generado por 2_consolidar_variables_sec.py
          (default: datos_sec_edgar/VARIABLES_FINANCIERAS_CRUDAS.csv)

Salida:   CSV listo para modelamiento
          (default: datos_sec_edgar/DATASET_MODELO_LISTO.csv)

Uso:
  python src/3_5_pipeline_completo.py --entrada <ruta_csv_crudo> --salida <ruta_csv_salida>
  python src/3_5_pipeline_completo.py  # usa rutas por defecto

Documentación: Docs/03_05_pipeline_unificado.md

Autor: Pipeline de riesgo financiero SEC DERA
Fecha: Abril 2026
Requisito previo: Ejecutar 2_consolidar_variables_sec.py
=============================================================================
"""

import os
import sys
import argparse
from pathlib import Path

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("Instalando dependencias...")
    os.system("pip install pandas numpy -q")
    import pandas as pd
    import numpy as np


# ═══════════════════════════════════════════════════════════════════════════════
#  CONSTANTES
# ═══════════════════════════════════════════════════════════════════════════════

# Columnas de metadata (no son datos financieros)
META_COLS = [
    'adsh', 'cik', 'name', 'sic', 'form', 'period',
    'filed', 'fy', 'fp', 'fye', 'countryba', 'stprba'
]

# Tags que son datos puntuales (balance) — NO deben tener qtrs > 0
TAGS_BALANCE = {
    'Assets', 'AssetsCurrent', 'AssetsNoncurrent',
    'CashAndCashEquivalentsAtCarryingValue',
    'RestrictedCashAndCashEquivalentsAtCarryingValue',
    'ShortTermInvestments', 'MarketableSecuritiesCurrent',
    'AccountsReceivableNetCurrent', 'InventoryNet',
    'PrepaidExpenseAndOtherAssetsCurrent',
    'PropertyPlantAndEquipmentNet', 'OperatingLeaseRightOfUseAsset',
    'Goodwill', 'IntangibleAssetsNetExcludingGoodwill',
    'OtherAssetsNoncurrent', 'OtherAssetsCurrent',
    'Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent',
    'AccountsPayableCurrent', 'AccruedLiabilitiesCurrent',
    'ShortTermBorrowings', 'DebtCurrent', 'LongTermDebt',
    'LongTermDebtNoncurrent', 'LongTermDebtCurrent',
    'OperatingLeaseLiabilityCurrent', 'OperatingLeaseLiabilityNoncurrent',
    'DeferredRevenueCurrent', 'DeferredRevenueNoncurrent',
    'ContractWithCustomerLiabilityCurrent',
    'OtherLiabilitiesNoncurrent', 'OtherLiabilitiesCurrent',
    'StockholdersEquity',
    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
    'RetainedEarningsAccumulatedDeficit',
    'CommonStockValue', 'AdditionalPaidInCapital',
    'TreasuryStockValue', 'MinorityInterest',
    'AccumulatedOtherComprehensiveIncomeLossNetOfTax',
    'CommonStockSharesOutstanding', 'CommonStockSharesIssued',
}

# Tags que son datos de flujo (IS/CF) — qtrs debe estar entre 1 y 4
TAGS_FLUJO = {
    'Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax',
    'CostOfRevenue', 'CostOfGoodsAndServicesSold', 'GrossProfit',
    'OperatingExpenses', 'ResearchAndDevelopmentExpense',
    'SellingGeneralAndAdministrativeExpense',
    'GeneralAndAdministrativeExpense', 'SellingAndMarketingExpense',
    'DepreciationAndAmortization', 'DepreciationDepletionAndAmortization',
    'Depreciation', 'AmortizationOfIntangibleAssets',
    'OperatingIncomeLoss', 'InterestExpense', 'InterestExpenseDebt',
    'InterestIncomeExpenseNet', 'NonoperatingIncomeExpense',
    'OtherNonoperatingIncomeExpense',
    'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
    'IncomeTaxExpenseBenefit', 'IncomeLossFromContinuingOperations',
    'NetIncomeLoss', 'ComprehensiveIncomeNetOfTax',
    'EarningsPerShareBasic', 'EarningsPerShareDiluted',
    'NetCashProvidedByUsedInOperatingActivities',
    'NetCashProvidedByUsedInInvestingActivities',
    'NetCashProvidedByUsedInFinancingActivities',
    'ShareBasedCompensation',
    'PaymentsToAcquirePropertyPlantAndEquipment',
    'ProceedsFromSaleOfPropertyPlantAndEquipment',
    'PaymentsToAcquireBusinessesNetOfCashAcquired',
    'PaymentsOfDividends', 'PaymentsOfDividendsCommonStock',
    'ProceedsFromIssuanceOfDebt', 'ProceedsFromIssuanceOfLongTermDebt',
    'RepaymentsOfDebt', 'RepaymentsOfLongTermDebt',
    'ProceedsFromIssuanceOfCommonStock', 'ProceedsFromStockOptionsExercised',
    'PaymentsForRepurchaseOfCommonStock',
    'IncreaseDecreaseInAccountsReceivable',
    'IncreaseDecreaseInInventories', 'IncreaseDecreaseInAccountsPayable',
    'DeferredIncomeTaxExpenseBenefit',
    'WeightedAverageNumberOfSharesOutstandingBasic',
    'WeightedAverageNumberOfDilutedSharesOutstanding',
}

# Columnas fe_ctx_ que SIEMPRE se conservan (son contextuales, no features del modelo)
CONTEXT_COLS_KEEP = [
    'fe_ctx_revenue_consolidado',
    'fe_ctx_fp_orden',
    'fe_ctx_periodo',
    'fe_ctx_filing_lag',
    'fe_ctx_amendment',
    'fe_ctx_sic_sector',
]

# Columnas fe_* que son targets / variables objetivo (NO son features)
TARGET_COLS = [
    'fe_zscore_altman',
    'fe_zscore_risk_score',
]

# Columnas fe_flag_ que derivan del Z-Score
ZSCORE_DERIVED_FLAGS = [
    'fe_flag_altman_distress',
    'fe_flag_altman_grey',
]

# Ratios que necesitan winsorización
RATIO_COLS_TO_WINSORIZE = [
    'fe_ratio_roa', 'fe_ratio_roe',
    'fe_ratio_margen_bruto', 'fe_ratio_margen_operativo',
    'fe_ratio_margen_neto', 'fe_ratio_ebitda_assets',
    'fe_ratio_liquidez', 'fe_ratio_quick',
    'fe_ratio_cash', 'fe_ratio_cash_current',
    'fe_ratio_apalancamiento', 'fe_ratio_deuda_equity',
    'fe_ratio_deuda_assets', 'fe_ratio_deuda_cp_total',
    'fe_ratio_cobertura_intereses', 'fe_ratio_rotacion_activos',
    'fe_ratio_sga_revenue', 'fe_ratio_rnd_revenue',
    'fe_ratio_capex_revenue', 'fe_ratio_tangibilidad',
    'fe_ratio_goodwill_assets', 'fe_ratio_intangibles_assets',
    'fe_ratio_capital_trabajo', 'fe_ratio_fcf_assets',
    'fe_ratio_calidad_ingresos', 'fe_ratio_cashflow_deuda',
    'fe_ratio_cfo_revenue',
]

# Columnas de shares para winsorizar
SHARES_COLS_TO_WINSORIZE = [
    'fe_shares_book_value',
    'fe_shares_dilution',
    'fe_shares_assets_per_share',
]


# ═══════════════════════════════════════════════════════════════════════════════
#  FUNCIONES DE UTILIDAD
# ═══════════════════════════════════════════════════════════════════════════════

def print_fase(num, titulo):
    """Imprime encabezado de una fase del pipeline."""
    print(f"\n{'█' * 80}")
    print(f"  FASE {num}: {titulo}")
    print(f"{'█' * 80}")


def print_step(num, titulo, descripcion=""):
    """Imprime el encabezado de un paso de limpieza o bloque de FE."""
    print(f"\n{'─' * 80}")
    print(f"  {num}: {titulo}")
    print(f"{'─' * 80}")
    if descripcion:
        print(f"  ¿POR QUÉ? {descripcion}")
        print()


def print_resultado(filas_antes, filas_despues, accion="eliminadas"):
    """Imprime el resultado de un paso."""
    diff = filas_antes - filas_despues
    print(f"  ✅ RESULTADO: {diff:,} filas {accion}")
    print(f"     Antes: {filas_antes:,} → Después: {filas_despues:,}")


def print_modificacion(n_modificadas, campo, accion="convertidas a NaN"):
    """Imprime resultado de una modificación en lugar de eliminación."""
    print(f"  ✅ RESULTADO: {n_modificadas:,} valores en '{campo}' {accion}")


def print_var(nombre, descripcion, cobertura_pct):
    """Imprime info de una variable creada."""
    bar = "#" * int(cobertura_pct / 5)
    print(f"    {nombre:50s} ({cobertura_pct:5.1f}%) {bar}  {descripcion}")


def safe_div(numerador, denominador, fill=np.nan):
    """
    Division segura: retorna fill cuando el denominador es 0 o NaN.
    Esto evita divisiones por cero y produce NaN en lugar de Inf.
    """
    num = pd.to_numeric(numerador, errors='coerce')
    den = pd.to_numeric(denominador, errors='coerce')
    result = np.where((den == 0) | den.isna() | num.isna(), fill, num / den)
    return pd.Series(result, index=numerador.index if hasattr(numerador, 'index') else None)


def winsorize_series(series, lower_pct, upper_pct):
    """
    Winsoriza una serie: recorta valores por debajo del percentil lower_pct
    y por encima del percentil upper_pct.
    Solo aplica a valores no-NaN.
    """
    if series.notna().sum() == 0:
        return series

    lower_bound = series.quantile(lower_pct / 100.0)
    upper_bound = series.quantile(upper_pct / 100.0)
    return series.clip(lower=lower_bound, upper=upper_bound)


# ═══════════════════════════════════════════════════════════════════════════════
#  FASE 1: VALIDACIÓN Y LIMPIEZA
# ═══════════════════════════════════════════════════════════════════════════════

def fase1_validar_y_limpiar(df):
    """
    Proceso de validación y limpieza en 9 pasos.

    Cada paso fue diseñado tras un análisis exploratorio exhaustivo del CSV.
    Los hallazgos se documentaron en: Docs/02_hallazgos_analisis_exploratorio.md

    Recibe el DataFrame crudo y retorna el DataFrame limpio.
    """

    print_fase(1, "VALIDACIÓN Y LIMPIEZA DE VARIABLES FINANCIERAS")

    n_original = len(df)
    print(f"  {n_original:,} filas × {len(df.columns)} columnas")
    print(f"  {df['cik'].nunique():,} empresas únicas")

    # Asegurar tipos numéricos en campos clave
    df['fy'] = pd.to_numeric(df['fy'], errors='coerce')
    df['period'] = pd.to_numeric(df['period'], errors='coerce')
    df['filed'] = pd.to_numeric(df['filed'], errors='coerce')

    # Identificar columnas de tags y qtrs presentes
    qtrs_cols = [c for c in df.columns if c.endswith('_qtrs')]
    tag_cols = [c for c in df.columns if c not in META_COLS and c not in qtrs_cols]

    # ── PASO 0: DIAGNÓSTICO INICIAL ──────────────────────────────────────────

    print_step("PASO 0", "DIAGNÓSTICO INICIAL",
        "Verificamos el estado de los datos antes de cualquier limpieza.")

    print("  Distribución de formularios:")
    for form, cnt in df['form'].value_counts().items():
        print(f"    {form:12s}: {cnt:>8,} ({cnt/len(df)*100:.1f}%)")

    print("\n  Distribución de períodos fiscales (fp):")
    for fp, cnt in df['fp'].value_counts().items():
        print(f"    {str(fp):6s}: {cnt:>8,} ({cnt/len(df)*100:.1f}%)")

    print(f"\n  Rango temporal:")
    print(f"    fy: {df['fy'].min():.0f} – {df['fy'].max():.0f}")
    print(f"    period: {df['period'].min():.0f} – {df['period'].max():.0f}")

    print("\n  Cobertura top 10 tags:")
    for tag in tag_cols:
        df[tag] = pd.to_numeric(df[tag], errors='coerce')

    cob = [(t, df[t].notna().sum(), df[t].notna().sum()/len(df)*100)
           for t in tag_cols if t in df.columns]
    cob.sort(key=lambda x: x[2], reverse=True)
    for tag, n, pct in cob[:10]:
        bar = "█" * int(pct / 5)
        print(f"    {tag:55s} {n:>8,} ({pct:5.1f}%) {bar}")

    # ── PASO 1: ELIMINAR REGISTROS CON fp = Q4 ──────────────────────────────

    print_step("PASO 1", "ELIMINAR REGISTROS CON fp = Q4",
        "En SEC EDGAR, 'Q4' es anómalo. El reporte anual (10-K) cubre el\n"
        "             cuarto trimestre completo y usa fp='FY'. Solo existen 6 registros\n"
        "             con fp='Q4', lo que indica errores en los datos de origen.")

    n_antes = len(df)
    df['fp'] = df['fp'].astype(str).str.strip()
    mask_q4 = df['fp'] == 'Q4'
    n_q4 = mask_q4.sum()

    if n_q4 > 0:
        print(f"  Encontrados: {n_q4} registros con fp='Q4'")
        print(f"  Empresas afectadas: {df[mask_q4]['cik'].nunique()}")
        df = df[~mask_q4].copy()

    print_resultado(n_antes, len(df))

    # ── PASO 2: ELIMINAR DUPLICADOS cik + fy + fp ────────────────────────────

    print_step("PASO 2", "ELIMINAR DUPLICADOS cik + fy + fp",
        "17,042 filas tienen el mismo cik+fy+fp. Esto ocurre cuando una\n"
        "             empresa presenta un filing original (10-K/10-Q) y luego una\n"
        "             enmienda (10-K/A o 10-Q/A) para el mismo período.\n"
        "             Nos quedamos con el filing más reciente (mayor 'filed'), ya que\n"
        "             la enmienda siempre contiene la versión corregida.")

    n_antes = len(df)

    dup_mask = df.duplicated(subset=['cik', 'fy', 'fp'], keep=False)
    n_dup_filas = dup_mask.sum()
    n_dup_grupos = df[dup_mask].groupby(['cik', 'fy', 'fp']).ngroups
    print(f"  Encontrados: {n_dup_filas:,} filas duplicadas en {n_dup_grupos:,} grupos")

    if n_dup_grupos > 0:
        print("  Ejemplos:")
        shown = 0
        for (cik, fy, fp), group in df[dup_mask].groupby(['cik', 'fy', 'fp']):
            if shown >= 3:
                break
            forms = group['form'].tolist()
            filed_dates = group['filed'].tolist()
            print(f"    cik={cik}, fy={fy:.0f}, fp={fp}: "
                  f"{len(group)} filas → forms={forms}, filed={filed_dates}")
            shown += 1

    df = df.sort_values(['cik', 'fy', 'fp', 'filed'])
    df = df.drop_duplicates(subset=['cik', 'fy', 'fp'], keep='last')

    print_resultado(n_antes, len(df))

    # ── PASO 3: ELIMINAR FILAS CON Assets NEGATIVOS ──────────────────────────

    print_step("PASO 3", "ELIMINAR FILAS CON Assets NEGATIVOS",
        "Los activos totales de una empresa NUNCA pueden ser negativos.\n"
        "             Esto indica un error en los datos XBRL de origen.")

    n_antes = len(df)
    assets = pd.to_numeric(df['Assets'], errors='coerce')
    mask_neg = assets < 0
    n_neg = mask_neg.sum()

    if n_neg > 0:
        print(f"  Encontrados: {n_neg} filas con Assets < 0")
        print(f"  Valores: {assets[mask_neg].tolist()}")
        df = df[~mask_neg].copy()

    print_resultado(n_antes, len(df))

    # ── PASO 4: ELIMINAR FILAS CON Assets = 0 ───────────────────────────────

    print_step("PASO 4", "ELIMINAR FILAS CON Assets = 0",
        "Típicamente 'shell companies', SPACs sin activos reales, o empresas\n"
        "             pre-operativas. No son útiles para modelar riesgo financiero y\n"
        "             provocarían divisiones por cero en los ratios.")

    n_antes = len(df)
    assets = pd.to_numeric(df['Assets'], errors='coerce')
    mask_zero = assets == 0
    n_zero = mask_zero.sum()

    if n_zero > 0:
        print(f"  Encontrados: {n_zero:,} filas con Assets = 0")
        df = df[~mask_zero].copy()

    print_resultado(n_antes, len(df))

    # ── PASO 5: ELIMINAR FILAS CON Assets NaN ────────────────────────────────

    print_step("PASO 5", "ELIMINAR FILAS CON Assets = NaN",
        "Sin activos totales, es imposible calcular la mayoría de los ratios\n"
        "             financieros (ROA, apalancamiento, Z-Score, etc.).")

    n_antes = len(df)
    assets = pd.to_numeric(df['Assets'], errors='coerce')
    mask_nan = assets.isna()
    n_nan = mask_nan.sum()

    if n_nan > 0:
        print(f"  Encontrados: {n_nan:,} filas con Assets = NaN")
        df = df[~mask_nan].copy()

    print_resultado(n_antes, len(df))

    # ── PASO 6: CORREGIR VALORES IMPOSIBLES ──────────────────────────────────

    print_step("PASO 6", "CORREGIR VALORES IMPOSIBLES EN CAMPOS ESPECÍFICOS",
        "Algunos campos tienen valores contablemente imposibles.\n"
        "             En lugar de eliminar la fila completa, convertimos solo\n"
        "             el valor problemático a NaN.")

    # Cash negativo → NaN
    col = 'CashAndCashEquivalentsAtCarryingValue'
    if col in df.columns:
        vals = pd.to_numeric(df[col], errors='coerce')
        mask = vals < 0
        n = mask.sum()
        if n > 0:
            df.loc[mask, col] = np.nan
            print_modificacion(n, col, "negativos convertidos a NaN")

    # Acciones negativas → NaN
    for col in ['CommonStockSharesOutstanding', 'CommonStockSharesIssued']:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors='coerce')
            mask = vals < 0
            n = mask.sum()
            if n > 0:
                df.loc[mask, col] = np.nan
                print_modificacion(n, col, "negativos convertidos a NaN")

    # Liabilities negativas → NaN
    col = 'Liabilities'
    if col in df.columns:
        vals = pd.to_numeric(df[col], errors='coerce')
        mask = vals < 0
        n = mask.sum()
        if n > 0:
            df.loc[mask, col] = np.nan
            print_modificacion(n, col, "negativos convertidos a NaN")

    # ── PASO 7: LIMPIAR DATOS DE FLUJO CON qtrs ANÓMALOS ────────────────────

    print_step("PASO 7", "LIMPIAR DATOS DE FLUJO CON qtrs ANÓMALOS",
        "Los datos de flujo usan 'qtrs' para indicar cobertura temporal.\n"
        "             Valores con qtrs > 4 o qtrs = 0 son errores XBRL.\n"
        "             Se convierten a NaN sin eliminar la fila.")

    n_total_limpiados = 0

    for tag in TAGS_FLUJO:
        qtrs_col = f"{tag}_qtrs"
        if tag in df.columns and qtrs_col in df.columns:
            qtrs = pd.to_numeric(df[qtrs_col], errors='coerce')

            mask_alta = qtrs > 4
            mask_cero = qtrs == 0
            mask_total = mask_alta | mask_cero
            n_total = mask_total.sum()

            if n_total > 0:
                df.loc[mask_total, tag] = np.nan
                df.loc[mask_total, qtrs_col] = np.nan
                n_total_limpiados += n_total

    print(f"  ✅ RESULTADO: {n_total_limpiados:,} valores de flujo con qtrs anómalo → NaN")
    print(f"     (Las filas se mantienen; solo se anulan los valores con qtrs inválido)")

    # ── PASO 8: FILTRAR POR RANGO TEMPORAL RAZONABLE ─────────────────────────

    print_step("PASO 8", "FILTRAR POR RANGO TEMPORAL RAZONABLE",
        "Filtramos fy < 2013 (datos demasiado antiguos) y\n"
        "             fy > 2025 (proyecciones a futuro).")

    n_antes = len(df)

    mask_viejo = df['fy'] < 2013
    mask_futuro = df['fy'] > 2025
    n_viejo = mask_viejo.sum()
    n_futuro = mask_futuro.sum()

    print(f"  Encontrados: {n_viejo:,} filas con fy < 2013, {n_futuro:,} filas con fy > 2025")

    df = df[(df['fy'] >= 2013) & (df['fy'] <= 2025)].copy()

    print_resultado(n_antes, len(df))

    # ── REPORTE FASE 1 ──────────────────────────────────────────────────────

    n_final = len(df)
    n_eliminadas = n_original - n_final
    pct_eliminadas = n_eliminadas / n_original * 100

    print(f"\n  {'═' * 70}")
    print(f"  ✅ FASE 1 COMPLETADA — VALIDACIÓN Y LIMPIEZA")
    print(f"  {'═' * 70}")
    print(f"     Original:   {n_original:,} filas")
    print(f"     Final:      {n_final:,} filas")
    print(f"     Eliminadas: {n_eliminadas:,} filas ({pct_eliminadas:.1f}%)")
    print(f"     Empresas:   {df['cik'].nunique():,} únicas")
    print(f"     Período:    fy {df['fy'].min():.0f} – {df['fy'].max():.0f}")

    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  FASE 2: FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════════════════════════

def fase2_feature_engineering(df):
    """
    Proceso de feature engineering en 7 bloques.
    Cada variable se construye con division segura y se documenta en consola.

    Recibe el DataFrame limpio y retorna el DataFrame enriquecido.
    """

    print_fase(2, "FEATURE ENGINEERING — Variables Derivadas")

    n_rows = len(df)
    n_cols_inicio = len(df.columns)
    print(f"  {n_rows:,} filas × {n_cols_inicio} columnas")

    # Asegurar tipos numéricos en campos clave
    numeric_candidates = [c for c in df.columns
                          if c not in ['adsh', 'name', 'form', 'fp', 'countryba', 'stprba', 'fye']]
    for col in numeric_candidates:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Contador de variables creadas
    vars_creadas = []

    def registrar(nombre, serie, descripcion):
        """Registra una variable creada y muestra su cobertura."""
        df[nombre] = serie
        pct = serie.notna().sum() / n_rows * 100
        vars_creadas.append((nombre, descripcion, pct))
        print_var(nombre, descripcion, pct)

    # ── BLOQUE 1: VARIABLES CONTEXTUALES (fe_ctx_) ───────────────────────────

    print_step("BLOQUE 1", "VARIABLES CONTEXTUALES (fe_ctx_) — 6 variables",
               "Derivan de la metadata del filing, no de datos financieros.")

    # 1.1 Consolidacion de revenues (pre/post ASC 606)
    rev1 = df.get('Revenues', pd.Series(np.nan, index=df.index))
    rev2 = df.get('RevenueFromContractWithCustomerExcludingAssessedTax',
                  pd.Series(np.nan, index=df.index))
    registrar('fe_ctx_revenue_consolidado',
              rev1.fillna(rev2),
              'Revenues unificado (pre/post ASC 606)')

    # 1.2 Orden del periodo fiscal
    fp_map = {'Q1': 1, 'Q2': 2, 'Q3': 3, 'FY': 4}
    registrar('fe_ctx_fp_orden',
              df['fp'].map(fp_map),
              'Orden numerico: Q1=1, Q2=2, Q3=3, FY=4')

    # 1.3 Identificador de periodo
    registrar('fe_ctx_periodo',
              df['fy'].astype(int).astype(str) + '-' + df['fp'].astype(str),
              'Periodo legible: "2021-Q1", "2021-FY"')

    # 1.4 Filing lag (dias entre cierre y presentacion)
    try:
        period_dt = pd.to_datetime(df['period'].astype(int).astype(str), format='%Y%m%d', errors='coerce')
        filed_dt = pd.to_datetime(df['filed'].astype(int).astype(str), format='%Y%m%d', errors='coerce')
        filing_lag = (filed_dt - period_dt).dt.days
        registrar('fe_ctx_filing_lag',
                  filing_lag,
                  'Dias entre cierre fiscal y presentacion')
    except Exception:
        print("    [WARN] No se pudo calcular filing_lag")

    # 1.5 Flag de enmienda
    registrar('fe_ctx_amendment',
              df['form'].str.contains('/A', na=False).astype(int),
              '1 si es enmienda (10-K/A o 10-Q/A)')

    # 1.6 Sector industrial (2 digitos SIC)
    registrar('fe_ctx_sic_sector',
              df['sic'].apply(lambda x: int(str(int(x))[:2]) if pd.notna(x) else np.nan),
              'Primeros 2 digitos SIC (sector industrial)')

    # ── BLOQUE 2: VALORES ANUALIZADOS (fe_anual_) ────────────────────────────

    print_step("BLOQUE 2", "VALORES ANUALIZADOS (fe_anual_) — 8 variables",
               "Los datos de flujo son acumulativos. Se anualizan con factor 4/qtrs.")

    anual_config = [
        ('fe_ctx_revenue_consolidado', 'fe_anual_revenue',
         None, 'Ingresos proyectados a 12 meses'),
        ('OperatingIncomeLoss', 'fe_anual_ebit',
         'OperatingIncomeLoss_qtrs', 'EBIT proyectado a 12 meses'),
        ('NetIncomeLoss', 'fe_anual_net_income',
         'NetIncomeLoss_qtrs', 'Resultado neto proyectado a 12 meses'),
        ('NetCashProvidedByUsedInOperatingActivities', 'fe_anual_cash_operating',
         'NetCashProvidedByUsedInOperatingActivities_qtrs', 'Cash Flow Operativo anualizado'),
        ('NetCashProvidedByUsedInInvestingActivities', 'fe_anual_cash_investing',
         'NetCashProvidedByUsedInInvestingActivities_qtrs', 'Cash Flow de Inversion anualizado'),
        ('NetCashProvidedByUsedInFinancingActivities', 'fe_anual_cash_financing',
         'NetCashProvidedByUsedInFinancingActivities_qtrs', 'Cash Flow de Financiamiento anualizado'),
        ('GrossProfit', 'fe_anual_gross_profit',
         'GrossProfit_qtrs', 'Utilidad Bruta proyectada a 12 meses'),
    ]

    for tag, nombre, qtrs_col, desc in anual_config:
        if tag in df.columns:
            valor = df[tag].copy()

            if qtrs_col and qtrs_col in df.columns:
                qtrs = df[qtrs_col]
            elif tag == 'fe_ctx_revenue_consolidado':
                rev_qtrs = df.get('Revenues_qtrs', pd.Series(np.nan, index=df.index))
                rev2_qtrs = df.get('RevenueFromContractWithCustomerExcludingAssessedTax_qtrs',
                                   pd.Series(np.nan, index=df.index))
                qtrs = rev_qtrs.fillna(rev2_qtrs)
            else:
                qtrs_candidate = f"{tag}_qtrs"
                qtrs = df.get(qtrs_candidate, pd.Series(np.nan, index=df.index))

            qtrs_numeric = pd.to_numeric(qtrs, errors='coerce')
            factor = np.where(
                (qtrs_numeric >= 1) & (qtrs_numeric <= 4),
                4.0 / qtrs_numeric,
                np.nan
            )

            resultado = pd.Series(
                np.where(pd.notna(valor), valor * factor, np.nan),
                index=df.index
            )
            registrar(nombre, resultado, desc)

    # Factor de anualizacion como referencia
    rev_qtrs = df.get('Revenues_qtrs', pd.Series(np.nan, index=df.index))
    rev2_qtrs = df.get('RevenueFromContractWithCustomerExcludingAssessedTax_qtrs',
                       pd.Series(np.nan, index=df.index))
    qtrs_ref = rev_qtrs.fillna(rev2_qtrs).fillna(df.get('NetIncomeLoss_qtrs', np.nan))
    qtrs_ref = pd.to_numeric(qtrs_ref, errors='coerce')
    factor_ref = np.where((qtrs_ref >= 1) & (qtrs_ref <= 4), 4.0 / qtrs_ref, np.nan)
    registrar('fe_anual_factor',
              pd.Series(factor_ref, index=df.index),
              'Factor de anualizacion aplicado (4/qtrs)')

    # ── BLOQUE 3: RATIOS FINANCIEROS (fe_ratio_) ────────────────────────────

    print_step("BLOQUE 3", "RATIOS FINANCIEROS (fe_ratio_) — 27 variables",
               "Todos usan division segura (retorna NaN si denominador = 0 o NaN).")

    # --- RENTABILIDAD ---
    print("  >> Rentabilidad:")

    registrar('fe_ratio_roa',
              safe_div(df['NetIncomeLoss'], df['Assets']),
              'ROA = NetIncome / Assets')

    registrar('fe_ratio_roe',
              safe_div(df['NetIncomeLoss'], df['StockholdersEquity']),
              'ROE = NetIncome / Equity')

    registrar('fe_ratio_margen_bruto',
              safe_div(df.get('GrossProfit'), df['fe_ctx_revenue_consolidado']),
              'Margen Bruto = GrossProfit / Revenue')

    registrar('fe_ratio_margen_operativo',
              safe_div(df['OperatingIncomeLoss'], df['fe_ctx_revenue_consolidado']),
              'Margen Operativo = EBIT / Revenue')

    registrar('fe_ratio_margen_neto',
              safe_div(df['NetIncomeLoss'], df['fe_ctx_revenue_consolidado']),
              'Margen Neto = NetIncome / Revenue')

    registrar('fe_ratio_ebitda_assets',
              safe_div(
                  df['OperatingIncomeLoss'].fillna(0) + df.get('DepreciationAndAmortization', pd.Series(0, index=df.index)).fillna(0),
                  df['Assets']
              ),
              'EBITDA / Assets (proxy)')

    # --- LIQUIDEZ ---
    print("\n  >> Liquidez:")

    registrar('fe_ratio_liquidez',
              safe_div(df['AssetsCurrent'], df['LiabilitiesCurrent']),
              'Current Ratio = ActivoCirc / PasivoCirc')

    quick_assets = df['AssetsCurrent'].fillna(0) - df.get('InventoryNet', pd.Series(0, index=df.index)).fillna(0)
    registrar('fe_ratio_quick',
              safe_div(quick_assets, df['LiabilitiesCurrent']),
              'Quick Ratio = (ActivoCirc - Inventario) / PasivoCirc')

    registrar('fe_ratio_cash',
              safe_div(df['CashAndCashEquivalentsAtCarryingValue'], df['Assets']),
              'Cash Ratio = Efectivo / Assets')

    registrar('fe_ratio_cash_current',
              safe_div(df['CashAndCashEquivalentsAtCarryingValue'], df['LiabilitiesCurrent']),
              'Cash / Pasivo Circulante')

    # --- APALANCAMIENTO Y DEUDA ---
    print("\n  >> Apalancamiento y Deuda:")

    registrar('fe_ratio_apalancamiento',
              safe_div(df['Liabilities'], df['Assets']),
              'Leverage = Pasivos / Activos')

    # Deuda total (evitando doble conteo)
    ltd = df.get('LongTermDebt', pd.Series(np.nan, index=df.index))
    ltd_nc = df.get('LongTermDebtNoncurrent', pd.Series(0, index=df.index)).fillna(0)
    ltd_c = df.get('LongTermDebtCurrent', pd.Series(0, index=df.index)).fillna(0)
    deuda_lp = np.where(ltd.notna(), ltd, ltd_nc + ltd_c)
    deuda_cp = (df.get('DebtCurrent', pd.Series(0, index=df.index)).fillna(0) +
                df.get('ShortTermBorrowings', pd.Series(0, index=df.index)).fillna(0))
    deuda_total = pd.Series(deuda_lp, index=df.index).fillna(0) + deuda_cp

    registrar('fe_ratio_deuda_equity',
              safe_div(deuda_total, df['StockholdersEquity']),
              'Deuda Total / Equity')

    registrar('fe_ratio_deuda_assets',
              safe_div(deuda_total, df['Assets']),
              'Deuda Total / Assets')

    registrar('fe_ratio_deuda_cp_total',
              safe_div(deuda_cp, deuda_total),
              'Deuda CP / Deuda Total (presion de pago)')

    registrar('fe_ratio_cobertura_intereses',
              safe_div(df['OperatingIncomeLoss'], df.get('InterestExpense')),
              'EBIT / InterestExpense')

    # --- EFICIENCIA OPERATIVA ---
    print("\n  >> Eficiencia Operativa:")

    registrar('fe_ratio_rotacion_activos',
              safe_div(df['fe_ctx_revenue_consolidado'], df['Assets']),
              'Asset Turnover = Revenue / Assets')

    registrar('fe_ratio_sga_revenue',
              safe_div(df.get('SellingGeneralAndAdministrativeExpense'), df['fe_ctx_revenue_consolidado']),
              'SGA / Revenue (eficiencia administrativa)')

    registrar('fe_ratio_rnd_revenue',
              safe_div(df.get('ResearchAndDevelopmentExpense'), df['fe_ctx_revenue_consolidado']),
              'R&D / Revenue (intensidad de innovacion)')

    registrar('fe_ratio_capex_revenue',
              safe_div(df.get('PaymentsToAcquirePropertyPlantAndEquipment'), df['fe_ctx_revenue_consolidado']),
              'CapEx / Revenue (intensidad de capital)')

    # --- CALIDAD DEL BALANCE ---
    print("\n  >> Calidad del Balance:")

    registrar('fe_ratio_tangibilidad',
              safe_div(df.get('PropertyPlantAndEquipmentNet'), df['Assets']),
              'PPE / Assets (tangibilidad)')

    registrar('fe_ratio_goodwill_assets',
              safe_div(df.get('Goodwill'), df['Assets']),
              'Goodwill / Assets (riesgo de impairment)')

    registrar('fe_ratio_intangibles_assets',
              safe_div(df.get('IntangibleAssetsNetExcludingGoodwill'), df['Assets']),
              'Intangibles / Assets')

    registrar('fe_ratio_capital_trabajo',
              safe_div(
                  df['AssetsCurrent'].fillna(0) - df['LiabilitiesCurrent'].fillna(0),
                  df['Assets']
              ),
              'Working Capital / Assets')

    # --- FLUJOS DE CAJA ---
    print("\n  >> Flujos de Caja:")

    cfo = df.get('NetCashProvidedByUsedInOperatingActivities', pd.Series(np.nan, index=df.index))
    capex = df.get('PaymentsToAcquirePropertyPlantAndEquipment', pd.Series(0, index=df.index)).fillna(0)
    fcf = cfo.fillna(np.nan) - capex

    registrar('fe_ratio_fcf_assets',
              safe_div(fcf, df['Assets']),
              'Free Cash Flow / Assets')

    registrar('fe_ratio_calidad_ingresos',
              safe_div(cfo, df['NetIncomeLoss']),
              'CFO / NetIncome (calidad de ganancias)')

    registrar('fe_ratio_cashflow_deuda',
              safe_div(cfo, deuda_total),
              'CFO / Deuda Total (capacidad de pago)')

    registrar('fe_ratio_cfo_revenue',
              safe_div(cfo, df['fe_ctx_revenue_consolidado']),
              'CFO / Revenue (conversion de ventas a cash)')

    # ── BLOQUE 4: COMPONENTES Z-SCORE (fe_zscore_) ──────────────────────────

    print_step("BLOQUE 4", "COMPONENTES Z-SCORE DE ALTMAN (fe_zscore_) — 7 variables",
               "Z' = 0.717*X1 + 0.847*X2 + 3.107*X3 + 0.420*X4 + 0.998*X5")

    # X1: Working Capital / Assets
    wc = df['AssetsCurrent'].fillna(0) - df['LiabilitiesCurrent'].fillna(0)
    registrar('fe_zscore_x1_wc_assets',
              safe_div(wc, df['Assets']),
              'X1 = Capital de Trabajo / Activos')

    # X2: Retained Earnings / Assets
    registrar('fe_zscore_x2_re_assets',
              safe_div(df['RetainedEarningsAccumulatedDeficit'], df['Assets']),
              'X2 = Utilidades Retenidas / Activos')

    # X3: EBIT / Assets (version anualizada)
    ebit_anual = df.get('fe_anual_ebit', df['OperatingIncomeLoss'])
    registrar('fe_zscore_x3_ebit_assets',
              safe_div(ebit_anual, df['Assets']),
              'X3 = EBIT anualizado / Activos')

    # X4: Equity / Liabilities
    registrar('fe_zscore_x4_equity_liab',
              safe_div(df['StockholdersEquity'], df['Liabilities'].abs()),
              'X4 = Patrimonio / |Pasivos|')

    # X5: Revenue / Assets (version anualizada)
    rev_anual = df.get('fe_anual_revenue', df['fe_ctx_revenue_consolidado'])
    registrar('fe_zscore_x5_rev_assets',
              safe_div(rev_anual, df['Assets']),
              'X5 = Ingresos anualizados / Activos')

    # Z-Score combinado
    x1 = df['fe_zscore_x1_wc_assets']
    x2 = df['fe_zscore_x2_re_assets']
    x3 = df['fe_zscore_x3_ebit_assets']
    x4 = df['fe_zscore_x4_equity_liab']
    x5 = df['fe_zscore_x5_rev_assets']

    zscore = (0.717 * x1.fillna(0) +
              0.847 * x2.fillna(0) +
              3.107 * x3.fillna(0) +
              0.420 * x4.fillna(0) +
              0.998 * x5.fillna(0))

    # Solo calculamos zscore donde al menos 3 de 5 componentes tienen dato
    n_componentes = (x1.notna().astype(int) + x2.notna().astype(int) +
                     x3.notna().astype(int) + x4.notna().astype(int) +
                     x5.notna().astype(int))
    zscore = zscore.where(n_componentes >= 3, np.nan)
    zscore = zscore.clip(-10, 15)

    registrar('fe_zscore_altman',
              zscore,
              'Z-Score de Altman (Z\' modificado, clip -10 a 15)')

    # Risk score 0-1 (transformacion sigmoide)
    risk_score = 1 / (1 + np.exp(zscore - 1.23))
    registrar('fe_zscore_risk_score',
              risk_score,
              'Sigmoid(Z) -> 0=bajo riesgo, 1=alto riesgo')

    # ── BLOQUE 5: FLAGS DE ALERTA (fe_flag_) ─────────────────────────────────

    print_step("BLOQUE 5", "FLAGS DE ALERTA (fe_flag_) — 10 variables",
               "Indicadores binarios (0/1) que señalan condiciones de riesgo.")

    registrar('fe_flag_patrimonio_negativo',
              (df['StockholdersEquity'] < 0).astype(int).where(df['StockholdersEquity'].notna(), np.nan),
              '1 si Equity < 0 (posible insolvencia tecnica)')

    registrar('fe_flag_perdida_neta',
              (df['NetIncomeLoss'] < 0).astype(int).where(df['NetIncomeLoss'].notna(), np.nan),
              '1 si NetIncome < 0 (la empresa pierde dinero)')

    registrar('fe_flag_deficit_acumulado',
              (df['RetainedEarningsAccumulatedDeficit'] < 0).astype(int).where(df['RetainedEarningsAccumulatedDeficit'].notna(), np.nan),
              '1 si Retained Earnings < 0 (acumula perdidas historicas)')

    registrar('fe_flag_liquidez_critica',
              (df['fe_ratio_liquidez'] < 1).astype(int).where(df['fe_ratio_liquidez'].notna(), np.nan),
              '1 si Current Ratio < 1 (no cubre deudas CP)')

    registrar('fe_flag_fco_negativo',
              (cfo < 0).astype(int).where(cfo.notna(), np.nan),
              '1 si Cash Flow Operativo < 0 (quema caja)')

    registrar('fe_flag_insolvencia',
              (df['Liabilities'] > df['Assets']).astype(int).where(df['Liabilities'].notna(), np.nan),
              '1 si Pasivos > Activos (insolvencia patrimonial)')

    registrar('fe_flag_margen_negativo',
              (df['OperatingIncomeLoss'] < 0).astype(int).where(df['OperatingIncomeLoss'].notna(), np.nan),
              '1 si EBIT < 0 (operacion no rentable)')

    registrar('fe_flag_sin_revenue',
              (df['fe_ctx_revenue_consolidado'].isna() | (df['fe_ctx_revenue_consolidado'] == 0)).astype(int),
              '1 si no hay ingresos (pre-revenue o error)')

    registrar('fe_flag_altman_distress',
              (df['fe_zscore_altman'] < 1.23).astype(int).where(df['fe_zscore_altman'].notna(), np.nan),
              '1 si Z-Score < 1.23 (zona de distress)')

    registrar('fe_flag_altman_grey',
              ((df['fe_zscore_altman'] >= 1.23) & (df['fe_zscore_altman'] < 2.90)).astype(int).where(df['fe_zscore_altman'].notna(), np.nan),
              '1 si Z-Score entre 1.23 y 2.90 (zona gris)')

    # ── BLOQUE 6: VARIABLES DE ACCIONES (fe_shares_) ─────────────────────────

    print_step("BLOQUE 6", "VARIABLES DE ACCIONES (fe_shares_) — 3 variables",
               "Metricas derivadas de datos de acciones en circulacion.")

    shares_out = df.get('CommonStockSharesOutstanding', pd.Series(np.nan, index=df.index))
    shares_basic = df.get('WeightedAverageNumberOfSharesOutstandingBasic', pd.Series(np.nan, index=df.index))
    shares_diluted = df.get('WeightedAverageNumberOfDilutedSharesOutstanding', pd.Series(np.nan, index=df.index))

    registrar('fe_shares_book_value',
              safe_div(df['StockholdersEquity'], shares_out),
              'Book Value per Share = Equity / Shares')

    registrar('fe_shares_dilution',
              safe_div(shares_diluted - shares_basic, shares_basic),
              'Tasa de dilucion = (Diluted - Basic) / Basic')

    registrar('fe_shares_assets_per_share',
              safe_div(df['Assets'], shares_out),
              'Activos por accion = Assets / Shares')

    # ── BLOQUE 7: VARIABLES TEMPORALES (fe_delta_) ───────────────────────────

    print_step("BLOQUE 7", "VARIABLES TEMPORALES (fe_delta_) — 12 variables",
               "Cambios trimestre a trimestre por empresa.")

    # Ordenar para que el shift funcione correctamente
    df = df.sort_values(['cik', 'fy', 'fe_ctx_fp_orden']).reset_index(drop=True)

    delta_config = [
        ('Assets', 'fe_delta_assets_qoq', 'Cambio % en Activos vs trimestre anterior'),
        ('fe_ctx_revenue_consolidado', 'fe_delta_revenue_qoq', 'Cambio % en Ingresos vs trimestre anterior'),
        ('NetIncomeLoss', 'fe_delta_net_income_qoq', 'Cambio % en Resultado Neto vs trimestre anterior'),
        ('CashAndCashEquivalentsAtCarryingValue', 'fe_delta_cash_qoq', 'Cambio % en Efectivo vs trimestre anterior'),
        ('Liabilities', 'fe_delta_liabilities_qoq', 'Cambio % en Pasivos vs trimestre anterior'),
        ('StockholdersEquity', 'fe_delta_equity_qoq', 'Cambio % en Patrimonio vs trimestre anterior'),
        ('fe_ratio_liquidez', 'fe_delta_liquidez_qoq', 'Cambio absoluto en Current Ratio vs anterior'),
        ('fe_ratio_apalancamiento', 'fe_delta_apalancamiento_qoq', 'Cambio absoluto en Leverage vs anterior'),
        ('fe_zscore_altman', 'fe_delta_zscore_qoq', 'Cambio absoluto en Z-Score vs anterior'),
        ('fe_zscore_risk_score', 'fe_delta_risk_score_qoq', 'Cambio absoluto en Risk Score vs anterior'),
    ]

    for tag, nombre, desc in delta_config:
        if tag in df.columns:
            prev = df.groupby('cik')[tag].shift(1)

            if tag in ['Assets', 'fe_ctx_revenue_consolidado', 'NetIncomeLoss',
                       'CashAndCashEquivalentsAtCarryingValue', 'Liabilities',
                       'StockholdersEquity']:
                delta = safe_div(df[tag] - prev, prev.abs())
                delta = delta.clip(-5, 5)
            else:
                delta = df[tag] - prev

            registrar(nombre, delta, desc)

    # Variables temporales adicionales
    registrar('fe_delta_risk_deterioro',
              (df.get('fe_delta_risk_score_qoq', pd.Series(np.nan, index=df.index)) > 0).astype(int).where(
                  df.get('fe_delta_risk_score_qoq', pd.Series(np.nan, index=df.index)).notna(), np.nan
              ),
              '1 si el riesgo empeoro vs trimestre anterior')

    registrar('fe_delta_risk_score_prev',
              df.groupby('cik')['fe_zscore_risk_score'].shift(1),
              'Risk Score del trimestre anterior')

    # ── REPORTE FASE 2 ──────────────────────────────────────────────────────

    n_cols_final = len(df.columns)
    n_fe = len(vars_creadas)

    print(f"\n  {'═' * 70}")
    print(f"  ✅ FASE 2 COMPLETADA — FEATURE ENGINEERING")
    print(f"  {'═' * 70}")
    print(f"     Columnas originales: {n_cols_inicio}")
    print(f"     Variables creadas:   {n_fe}")
    print(f"     Columnas finales:    {n_cols_final}")

    prefixes = ['fe_ctx_', 'fe_anual_', 'fe_ratio_', 'fe_zscore_', 'fe_flag_', 'fe_shares_', 'fe_delta_']
    for prefix in prefixes:
        count = sum(1 for v, _, _ in vars_creadas if v.startswith(prefix))
        print(f"       {prefix:20s}: {count:3d} variables")

    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  FASE 3: PREPARACIÓN DEL DATASET
# ═══════════════════════════════════════════════════════════════════════════════

def fase3_preparar_dataset(df, min_cobertura, min_assets, min_periodos,
                           winsor_lower, winsor_upper):
    """
    Proceso de preparación del dataset en 5 sub-pasos.

    Recibe el DataFrame enriquecido y retorna el DataFrame listo para modelo.
    """

    print_fase(3, "PREPARACIÓN DEL DATASET PARA MODELAMIENTO")

    print(f"  Min cobertura:  {min_cobertura}%")
    print(f"  Min assets:     ${min_assets:,}")
    print(f"  Min períodos:   {min_periodos}")
    print(f"  Winsorización:  [{winsor_lower}%, {winsor_upper}%]")

    n_rows_inicio = len(df)
    n_cols_inicio = len(df.columns)
    n_empresas_inicio = df['cik'].nunique()
    print(f"  {n_rows_inicio:,} filas × {n_cols_inicio} columnas, {n_empresas_inicio:,} empresas")

    # ── 5.1: ELIMINACIÓN DE COLUMNAS INNECESARIAS ────────────────────────────

    print_step("PASO 5.1", "ELIMINACIÓN DE COLUMNAS INNECESARIAS",
               "Eliminar _qtrs auxiliares (ya usadas para anualizar en Fase 2).")

    cols_antes = len(df.columns)

    # Identificar columnas _qtrs
    qtrs_cols = [c for c in df.columns if c.endswith('_qtrs')]
    print(f"    Columnas _qtrs encontradas:  {len(qtrs_cols)}")

    # Identificar fe_* y XBRL crudas
    fe_cols = [c for c in df.columns if c.startswith('fe_')]
    xbrl_raw_cols = [c for c in df.columns
                     if c not in META_COLS
                     and not c.startswith('fe_')
                     and not c.endswith('_qtrs')]
    print(f"    Columnas XBRL crudas (conservadas): {len(xbrl_raw_cols)}")

    # Mantener: metadata + crudas + fe_*
    cols_to_keep = META_COLS.copy()
    cols_to_keep.extend([c for c in fe_cols if c in df.columns])
    cols_to_keep.extend([c for c in xbrl_raw_cols if c in df.columns])
    cols_to_keep = [c for c in cols_to_keep if c in df.columns]
    cols_to_keep = list(dict.fromkeys(cols_to_keep))

    df = df[cols_to_keep]
    cols_eliminadas = cols_antes - len(df.columns)

    print(f"    Columnas auxiliares eliminadas (_qtrs): {cols_eliminadas}")
    print(f"    Columnas restantes:                     {len(df.columns)}")

    # ── 5.2: FILTRO DE VARIABLES CON BAJA COBERTURA ──────────────────────────

    print_step("PASO 5.2", f"FILTRO DE VARIABLES CON COBERTURA < {min_cobertura}%",
               "Variables fe_* con demasiados NaN introducen más ruido que señal.")

    n_rows = len(df)
    fe_cols_current = [c for c in df.columns if c.startswith('fe_')]

    coberturas = {}
    for col in fe_cols_current:
        pct = df[col].notna().sum() / n_rows * 100
        coberturas[col] = pct

    cols_protegidas = set(CONTEXT_COLS_KEEP + TARGET_COLS + ZSCORE_DERIVED_FLAGS)
    cols_baja_cobertura = []
    for col, pct in coberturas.items():
        if pct < min_cobertura and col not in cols_protegidas:
            cols_baja_cobertura.append((col, pct))

    cols_baja_cobertura.sort(key=lambda x: x[1])

    print(f"\n    Variables fe_* analizadas:     {len(fe_cols_current)}")
    print(f"    Variables protegidas:          {len(cols_protegidas)}")
    print(f"    Variables con cobertura baja:  {len(cols_baja_cobertura)}")

    if cols_baja_cobertura:
        print(f"\n    Variables eliminadas por cobertura < {min_cobertura}%:")
        for col, pct in cols_baja_cobertura:
            print(f"      {col:50s} {pct:5.1f}%")

        cols_to_drop = [c for c, _ in cols_baja_cobertura]
        df = df.drop(columns=cols_to_drop)

    print(f"\n    Columnas restantes:            {len(df.columns)}")

    # ── 5.3: FILTROS DE CALIDAD ──────────────────────────────────────────────

    print_step("PASO 5.3", "FILTROS DE CALIDAD",
               f"Activos mínimos = ${min_assets:,} | Períodos mínimos por empresa = {min_periodos}")

    filas_antes = len(df)
    empresas_antes = df['cik'].nunique()

    # Filtrar por activos mínimos
    print(f"\n    Aplicando filtro de activos mínimos...")
    df['Assets'] = pd.to_numeric(df['Assets'], errors='coerce')
    mask_assets = df['Assets'] >= min_assets
    filas_eliminadas_assets = (~mask_assets).sum()
    df = df[mask_assets].copy()
    print(f"    Filas con Assets < ${min_assets:,}: {filas_eliminadas_assets:,} eliminadas")

    # Filtrar empresas con pocos períodos
    periodos_por_empresa = df.groupby('cik')['adsh'].count()
    empresas_pocas = periodos_por_empresa[periodos_por_empresa < min_periodos].index
    filas_eliminadas_periodos = df[df['cik'].isin(empresas_pocas)].shape[0]

    if len(empresas_pocas) > 0:
        df = df[~df['cik'].isin(empresas_pocas)].copy()
        print(f"    Empresas con < {min_periodos} períodos: {len(empresas_pocas):,} ({filas_eliminadas_periodos:,} filas)")

    filas_despues = len(df)
    empresas_despues = df['cik'].nunique()

    print(f"\n    Filas:    {filas_antes:,} → {filas_despues:,} ({filas_antes - filas_despues:,} eliminadas)")
    print(f"    Empresas: {empresas_antes:,} → {empresas_despues:,} ({empresas_antes - empresas_despues:,} eliminadas)")

    # ── 5.4: CONTROL DE OUTLIERS (WINSORIZACIÓN) ────────────────────────────

    print_step("PASO 5.4", f"CONTROL DE OUTLIERS — Winsorización [{winsor_lower}%, {winsor_upper}%]",
               "Recortar valores extremos en ratios para evitar distorsiones.")

    all_winsor_cols = RATIO_COLS_TO_WINSORIZE + SHARES_COLS_TO_WINSORIZE
    winsor_cols_present = [c for c in all_winsor_cols if c in df.columns]
    winsor_cols_absent = [c for c in all_winsor_cols if c not in df.columns]

    print(f"\n    Columnas a winsorizar: {len(winsor_cols_present)}")
    if winsor_cols_absent:
        print(f"    Columnas no presentes (ya eliminadas por cobertura): {len(winsor_cols_absent)}")

    n_winsorized = 0
    for col in winsor_cols_present:
        antes_min = df[col].min()
        antes_max = df[col].max()

        df[col] = winsorize_series(df[col], winsor_lower, winsor_upper)

        despues_min = df[col].min()
        despues_max = df[col].max()

        cambio = antes_min != despues_min or antes_max != despues_max
        if cambio:
            n_winsorized += 1
            print(f"    ✓ {col:50s} [{antes_min:12.2f}, {antes_max:12.2f}] → [{despues_min:12.2f}, {despues_max:12.2f}]")

    print(f"\n    Columnas efectivamente modificadas: {n_winsorized}/{len(winsor_cols_present)}")

    # ── 5.5: ANÁLISIS DE DISTRIBUCIÓN FINAL ──────────────────────────────────

    print_step("PASO 5.5", "ANÁLISIS DE DISTRIBUCIÓN FINAL")

    n_rows_final = len(df)
    n_cols_final = len(df.columns)
    n_empresas_final = df['cik'].nunique()

    # Distribución de fe_zscore_risk_score
    print(f"\n  ── DISTRIBUCIÓN DE fe_zscore_risk_score ─────────────────────")
    if 'fe_zscore_risk_score' in df.columns:
        risk = df['fe_zscore_risk_score'].dropna()
        print(f"    Valores válidos: {len(risk):,} ({len(risk)/n_rows_final*100:.1f}%)")
        print(f"    Media:           {risk.mean():.4f}")
        print(f"    Mediana:         {risk.median():.4f}")
        print(f"    Std:             {risk.std():.4f}")
        print(f"    Min / Max:       {risk.min():.4f} / {risk.max():.4f}")

        print(f"\n    Distribución por nivel de riesgo:")
        bins = [0, 0.20, 0.45, 0.65, 0.85, 1.01]
        labels = ['🟢 Bajo (0-0.20)', '🔵 Moderado-Bajo (0.20-0.45)',
                  '🟡 Moderado-Alto (0.45-0.65)', '🟠 Alto (0.65-0.85)',
                  '🔴 Crítico (0.85-1.00)']
        risk_cats = pd.cut(risk, bins=bins, labels=labels, right=False)
        for label in labels:
            cnt = (risk_cats == label).sum()
            pct = cnt / len(risk) * 100
            bar = "█" * int(pct / 2)
            print(f"      {label:40s}: {cnt:7,} ({pct:5.1f}%) {bar}")

    # Distribución de flags
    print(f"\n  ── DISTRIBUCIÓN DE FLAGS DE ALERTA ──────────────────────────")
    flag_cols = [c for c in df.columns if c.startswith('fe_flag_')]
    for col in flag_cols:
        n_valid = df[col].notna().sum()
        n_flagged = (df[col] == 1).sum()
        pct = n_flagged / n_valid * 100 if n_valid > 0 else 0
        print(f"    {col:45s}: {n_flagged:7,} / {n_valid:,} ({pct:5.1f}%)")

    # Distribución de Z-Score
    print(f"\n  ── DISTRIBUCIÓN DE fe_zscore_altman ─────────────────────────")
    if 'fe_zscore_altman' in df.columns:
        zscore = df['fe_zscore_altman'].dropna()
        n_distress = (zscore < 1.23).sum()
        n_grey = ((zscore >= 1.23) & (zscore < 2.90)).sum()
        n_safe = (zscore >= 2.90).sum()
        total = len(zscore)

        pct_d = n_distress / total * 100
        pct_g = n_grey / total * 100
        pct_s = n_safe / total * 100

        print(f"    DISTRESS (Z < 1.23) : {n_distress:7,} ({pct_d:5.1f}%) {'█' * int(pct_d / 2)}")
        print(f"    GREY (1.23-2.90)    : {n_grey:7,} ({pct_g:5.1f}%) {'█' * int(pct_g / 2)}")
        print(f"    SAFE (Z >= 2.90)    : {n_safe:7,} ({pct_s:5.1f}%) {'█' * int(pct_s / 2)}")

    # Cobertura final
    print(f"\n  ── COBERTURA FINAL DE VARIABLES fe_* ────────────────────────")
    fe_cols_final = [c for c in df.columns if c.startswith('fe_')]
    coberturas_final = []
    for col in fe_cols_final:
        pct = df[col].notna().sum() / n_rows_final * 100
        coberturas_final.append((col, pct))

    coberturas_final.sort(key=lambda x: x[1], reverse=True)
    for col, pct in coberturas_final:
        bar = "#" * int(pct / 5)
        print(f"    {col:50s} {pct:5.1f}% {bar}")

    # ── REPORTE FASE 3 ──────────────────────────────────────────────────────

    print(f"\n  {'═' * 70}")
    print(f"  ✅ FASE 3 COMPLETADA — DATASET LISTO PARA MODELAMIENTO")
    print(f"  {'═' * 70}")
    print(f"     Filas:     {n_rows_inicio:,} → {n_rows_final:,} ({n_rows_inicio - n_rows_final:,} eliminadas)")
    print(f"     Columnas:  {n_cols_inicio} → {n_cols_final} ({n_cols_inicio - n_cols_final} eliminadas)")
    print(f"     Empresas:  {n_empresas_inicio:,} → {n_empresas_final:,}")

    prefixes = ['fe_ctx_', 'fe_anual_', 'fe_ratio_', 'fe_zscore_',
                'fe_flag_', 'fe_shares_', 'fe_delta_']
    metadata_count = len([c for c in df.columns if c in META_COLS])
    print(f"\n     CATEGORÍAS DE VARIABLES FINALES:")
    print(f"     {'─' * 60}")
    print(f"       {'metadata':20s}: {metadata_count:3d} columnas")
    for prefix in prefixes:
        count = len([c for c in df.columns if c.startswith(prefix)])
        if count > 0:
            print(f"       {prefix:20s}: {count:3d} columnas")
    total_fe = len([c for c in df.columns if c.startswith('fe_')])
    xbrl_count = len([c for c in df.columns if c not in META_COLS and not c.startswith('fe_')])
    print(f"       {'xbrl_crudos':20s}: {xbrl_count:3d} columnas")
    print(f"     {'─' * 60}")
    print(f"       {'TOTAL':20s}: {n_cols_final:3d} columnas")

    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  PIPELINE COMPLETO
# ═══════════════════════════════════════════════════════════════════════════════

def ejecutar_pipeline(input_path: Path, output_path: Path,
                      min_cobertura: float, min_assets: int,
                      min_periodos: int, winsor_lower: float,
                      winsor_upper: float):
    """
    Ejecuta el pipeline completo de 3 fases sobre un único DataFrame
    en memoria, sin CSVs intermedios.

    Args:
        input_path:     Ruta del CSV crudo (VARIABLES_FINANCIERAS_CRUDAS.csv)
        output_path:    Ruta del CSV de salida (DATASET_MODELO_LISTO.csv)
        min_cobertura:  Cobertura mínima (%) para mantener una variable fe_*
        min_assets:     Activos mínimos (USD) para excluir shell companies
        min_periodos:   Períodos mínimos por empresa
        winsor_lower:   Percentil inferior para winsorización
        winsor_upper:   Percentil superior para winsorización
    """

    print("\n" + "█" * 80)
    print("  SEC DERA — PIPELINE COMPLETO")
    print("  Validación → Feature Engineering → Preparación del Dataset")
    print("█" * 80)
    print(f"  Entrada:  {input_path}")
    print(f"  Salida:   {output_path}")

    # ── CARGA ────────────────────────────────────────────────────────────────
    print(f"\n  📂 Cargando CSV crudo...")
    df = pd.read_csv(input_path, low_memory=False)
    n_original = len(df)
    n_cols_original = len(df.columns)
    print(f"     {n_original:,} filas × {n_cols_original} columnas cargadas")

    # ── FASE 1 ───────────────────────────────────────────────────────────────
    df = fase1_validar_y_limpiar(df)

    # ── FASE 2 ───────────────────────────────────────────────────────────────
    df = fase2_feature_engineering(df)

    # ── FASE 3 ───────────────────────────────────────────────────────────────
    df = fase3_preparar_dataset(
        df,
        min_cobertura=min_cobertura,
        min_assets=min_assets,
        min_periodos=min_periodos,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
    )

    # ── EXPORTACIÓN ──────────────────────────────────────────────────────────
    print(f"\n{'█' * 80}")
    print(f"  EXPORTACIÓN FINAL")
    print(f"{'█' * 80}")

    print(f"\n  💾 Guardando CSV final...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    tamano_mb = output_path.stat().st_size / (1024 * 1024)

    n_final = len(df)
    n_cols_final = len(df.columns)

    print(f"\n  {'═' * 70}")
    print(f"  ✅ PIPELINE COMPLETO FINALIZADO")
    print(f"  {'═' * 70}")
    print(f"     📄 Archivo:    {output_path}")
    print(f"     📊 Original:   {n_original:,} filas × {n_cols_original} cols")
    print(f"     📊 Final:      {n_final:,} filas × {n_cols_final} cols")
    print(f"     📊 Eliminadas: {n_original - n_final:,} filas ({(1 - n_final/n_original)*100:.1f}%)")
    print(f"     🏢 Empresas:   {df['cik'].nunique():,} únicas")
    print(f"     📅 Período:    fy {df['fy'].min():.0f} – {df['fy'].max():.0f}")
    print(f"     💾 Tamaño:     {tamano_mb:.1f} MB")

    print(f"\n     FLUJO EJECUTADO:")
    print(f"     {'─' * 60}")
    print(f"     FASE 1: Validación y Limpieza          (9 pasos)")
    print(f"     FASE 2: Feature Engineering             (7 bloques, ~73 vars)")
    print(f"     FASE 3: Preparación del Dataset         (5 sub-pasos)")
    print(f"     {'─' * 60}")
    print(f"     Sin CSVs intermedios — todo en memoria.")

    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="SEC DERA — Pipeline completo: Validación + Feature Engineering + Preparación del Dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python src/3_5_pipeline_completo.py
  python src/3_5_pipeline_completo.py --entrada datos/CRUDAS.csv --salida datos/MODELO.csv
  python src/3_5_pipeline_completo.py --min-assets 500000 --winsor-lower 2.0 --winsor-upper 98.0
        """
    )
    parser.add_argument('--entrada', default=None,
                        help='Ruta del CSV crudo de entrada (default: datos_sec_edgar/VARIABLES_FINANCIERAS_CRUDAS.csv)')
    parser.add_argument('--salida', default=None,
                        help='Ruta del CSV de salida listo para modelo (default: datos_sec_edgar/DATASET_MODELO_LISTO.csv)')
    parser.add_argument('--directorio', default='datos_sec_edgar',
                        help='Directorio de datos (default: datos_sec_edgar)')
    parser.add_argument('--min-cobertura', type=float, default=15.0,
                        help='Cobertura mínima (%%) para mantener una variable fe_* (default: 15.0)')
    parser.add_argument('--min-assets', type=int, default=100_000,
                        help='Activos mínimos en USD para excluir shell companies (default: 100000)')
    parser.add_argument('--min-periodos', type=int, default=2,
                        help='Períodos mínimos por empresa para mantenerla (default: 2)')
    parser.add_argument('--winsor-lower', type=float, default=1.0,
                        help='Percentil inferior para winsorización de ratios (default: 1.0)')
    parser.add_argument('--winsor-upper', type=float, default=99.0,
                        help='Percentil superior para winsorización de ratios (default: 99.0)')

    args = parser.parse_args()
    directorio = Path(args.directorio)

    if args.entrada:
        entrada = Path(args.entrada)
    else:
        entrada = directorio / "VARIABLES_FINANCIERAS_CRUDAS.csv"

    if args.salida:
        salida = Path(args.salida)
    else:
        salida = directorio / "DATASET_MODELO_LISTO.csv"

    if not entrada.exists():
        print(f"  ❌ Archivo no encontrado: {entrada}")
        print(f"  Ejecute primero: python src/2_consolidar_variables_sec.py")
        sys.exit(1)

    ejecutar_pipeline(
        entrada, salida,
        min_cobertura=args.min_cobertura,
        min_assets=args.min_assets,
        min_periodos=args.min_periodos,
        winsor_lower=args.winsor_lower,
        winsor_upper=args.winsor_upper,
    )

    print("\n  ✅ ¡Proceso completado!\n")


if __name__ == "__main__":
    main()
