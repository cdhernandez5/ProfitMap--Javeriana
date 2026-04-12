#!/usr/bin/env python3
"""
=============================================================================
SEC DERA — Consolidador de Variables Financieras Crudas
=============================================================================

Lee todos los trimestres descargados y genera UN SOLO CSV con todas las
variables financieras relevantes, SIN cálculos derivados.

Corrige problemas del consolidador original:
  1. Filtra 'segments' → solo datos consolidados (no por segmento)
  2. Filtra 'coreg'    → solo empresa principal (no subsidiarias)
  3. Maneja 'uom'      → USD para monetarios, shares para conteos
  4. Dedup inteligente  → NO usa aggfunc='first', sino lógica explícita
  5. ~70+ tags XBRL     → más del doble que el original

Uso:
  python consolidar_variables_sec.py
  python consolidar_variables_sec.py --inicio 2014Q1 --fin 2025Q4
  python consolidar_variables_sec.py --directorio datos_sec_edgar
=============================================================================
"""

import os
import sys
import argparse
from pathlib import Path

try:
    import pandas as pd
    from tqdm import tqdm
except ImportError:
    print("Instalando dependencias...")
    os.system("pip install pandas tqdm -q")
    import pandas as pd
    from tqdm import tqdm


# ═══════════════════════════════════════════════════════════════════════════
#  TAGS XBRL ORGANIZADOS POR ESTADO FINANCIERO Y TIPO DE UOM
# ═══════════════════════════════════════════════════════════════════════════

# --- Balance General (BS) — datos puntuales, qtrs=0 ---
TAGS_BALANCE = {
    # Activos
    'Assets',
    'AssetsCurrent',
    'AssetsNoncurrent',
    'CashAndCashEquivalentsAtCarryingValue',
    'RestrictedCashAndCashEquivalentsAtCarryingValue',
    'ShortTermInvestments',
    'MarketableSecuritiesCurrent',
    'AccountsReceivableNetCurrent',
    'InventoryNet',
    'PrepaidExpenseAndOtherAssetsCurrent',
    'PropertyPlantAndEquipmentNet',
    'OperatingLeaseRightOfUseAsset',
    'Goodwill',
    'IntangibleAssetsNetExcludingGoodwill',
    'OtherAssetsNoncurrent',
    'OtherAssetsCurrent',
    # Pasivos
    'Liabilities',
    'LiabilitiesCurrent',
    'LiabilitiesNoncurrent',
    'AccountsPayableCurrent',
    'AccruedLiabilitiesCurrent',
    'ShortTermBorrowings',
    'DebtCurrent',
    'LongTermDebt',
    'LongTermDebtNoncurrent',
    'LongTermDebtCurrent',
    'OperatingLeaseLiabilityCurrent',
    'OperatingLeaseLiabilityNoncurrent',
    'DeferredRevenueCurrent',
    'DeferredRevenueNoncurrent',
    'ContractWithCustomerLiabilityCurrent',
    'OtherLiabilitiesNoncurrent',
    'OtherLiabilitiesCurrent',
    # Patrimonio
    'StockholdersEquity',
    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
    'RetainedEarningsAccumulatedDeficit',
    'CommonStockValue',
    'AdditionalPaidInCapital',
    'TreasuryStockValue',
    'MinorityInterest',
    'AccumulatedOtherComprehensiveIncomeLossNetOfTax',
}

# --- Estado de Resultados (IS) — datos acumulados, qtrs>0 ---
TAGS_INCOME = {
    'Revenues',
    'RevenueFromContractWithCustomerExcludingAssessedTax',
    'CostOfRevenue',
    'CostOfGoodsAndServicesSold',
    'GrossProfit',
    'OperatingExpenses',
    'ResearchAndDevelopmentExpense',
    'SellingGeneralAndAdministrativeExpense',
    'GeneralAndAdministrativeExpense',
    'SellingAndMarketingExpense',
    'DepreciationAndAmortization',
    'DepreciationDepletionAndAmortization',
    'Depreciation',
    'AmortizationOfIntangibleAssets',
    'OperatingIncomeLoss',
    'InterestExpense',
    'InterestExpenseDebt',
    'InterestIncomeExpenseNet',
    'NonoperatingIncomeExpense',
    'OtherNonoperatingIncomeExpense',
    'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
    'IncomeTaxExpenseBenefit',
    'IncomeLossFromContinuingOperations',
    'NetIncomeLoss',
    'ComprehensiveIncomeNetOfTax',
    'EarningsPerShareBasic',
    'EarningsPerShareDiluted',
}

# --- Flujo de Efectivo (CF) — datos acumulados, qtrs>0 ---
TAGS_CASHFLOW = {
    'NetCashProvidedByUsedInOperatingActivities',
    'NetCashProvidedByUsedInInvestingActivities',
    'NetCashProvidedByUsedInFinancingActivities',
    'DepreciationDepletionAndAmortization',
    'ShareBasedCompensation',
    'PaymentsToAcquirePropertyPlantAndEquipment',
    'ProceedsFromSaleOfPropertyPlantAndEquipment',
    'PaymentsToAcquireBusinessesNetOfCashAcquired',
    'PaymentsOfDividends',
    'PaymentsOfDividendsCommonStock',
    'ProceedsFromIssuanceOfDebt',
    'ProceedsFromIssuanceOfLongTermDebt',
    'RepaymentsOfDebt',
    'RepaymentsOfLongTermDebt',
    'ProceedsFromIssuanceOfCommonStock',
    'ProceedsFromStockOptionsExercised',
    'PaymentsForRepurchaseOfCommonStock',
    'IncreaseDecreaseInAccountsReceivable',
    'IncreaseDecreaseInInventories',
    'IncreaseDecreaseInAccountsPayable',
    'DeferredIncomeTaxExpenseBenefit',
}

# --- Tags con uom='shares' (no USD) ---
TAGS_SHARES = {
    'CommonStockSharesOutstanding',
    'CommonStockSharesIssued',
    'WeightedAverageNumberOfSharesOutstandingBasic',
    'WeightedAverageNumberOfDilutedSharesOutstanding',
}

# Todos los tags monetarios (uom='USD')
TAGS_USD = TAGS_BALANCE | TAGS_INCOME | TAGS_CASHFLOW
# Todos los tags
ALL_TAGS = TAGS_USD | TAGS_SHARES

# Columnas a extraer de sub.csv
SUB_COLS = [
    'adsh', 'cik', 'name', 'sic', 'form', 'period',
    'filed', 'fy', 'fp', 'fye', 'countryba', 'stprba'
]

# Formularios válidos
FORMS_VALID = {'10-K', '10-Q', '10-K/A', '10-Q/A'}


# ═══════════════════════════════════════════════════════════════════════════
#  FUNCIONES
# ═══════════════════════════════════════════════════════════════════════════

def generar_trimestres(inicio: str, fin: str) -> list:
    """Genera lista de trimestres entre inicio y fin. Ej: '2014Q1' → [('2014','1'), ...]"""
    anio_ini, q_ini = int(inicio[:4]), int(inicio[-1])
    anio_fin, q_fin = int(fin[:4]), int(fin[-1])
    trimestres = []
    anio, q = anio_ini, q_ini
    while (anio < anio_fin) or (anio == anio_fin and q <= q_fin):
        trimestres.append((str(anio), str(q)))
        q += 1
        if q > 4:
            q = 1
            anio += 1
    return trimestres


def procesar_trimestre(dir_q: Path) -> pd.DataFrame | None:
    """
    Procesa un trimestre: lee sub.csv y num.csv, aplica todos los filtros,
    resuelve duplicados y retorna un DataFrame limpio en formato largo.
    """
    sub_path = dir_q / "sub.csv"
    num_path = dir_q / "num.csv"

    if not sub_path.exists() or not num_path.exists():
        return None

    # --- Leer sub.csv ---
    sub = pd.read_csv(sub_path, low_memory=False, dtype={'cik': str, 'sic': str})
    cols_disponibles = [c for c in SUB_COLS if c in sub.columns]
    sub = sub[cols_disponibles]
    sub = sub[sub['form'].isin(FORMS_VALID)]

    if len(sub) == 0:
        return None

    # Asegurar que period sea numérico para comparar con ddate
    sub['period'] = pd.to_numeric(sub['period'], errors='coerce')

    # --- Leer num.csv ---
    num = pd.read_csv(num_path, low_memory=False)

    # FILTRO 1: Solo tags relevantes
    num = num[num['tag'].isin(ALL_TAGS)]

    if len(num) == 0:
        return None

    # FILTRO 2: Solo datos consolidados (segments vacío/NaN)
    if 'segments' in num.columns:
        num = num[num['segments'].isna() | (num['segments'].astype(str).str.strip() == '')]

    # FILTRO 3: Solo empresa principal (coreg vacío/NaN)
    if 'coreg' in num.columns:
        num = num[num['coreg'].isna() | (num['coreg'].astype(str).str.strip() == '')]

    # FILTRO 4: UOM inteligente — USD para monetarios, shares para conteos
    num_usd = num[num['tag'].isin(TAGS_USD) & (num['uom'] == 'USD')]
    num_shares = num[num['tag'].isin(TAGS_SHARES) & (num['uom'] == 'shares')]
    num = pd.concat([num_usd, num_shares], ignore_index=True)

    if len(num) == 0:
        return None

    # Asegurar tipos
    num['ddate'] = pd.to_numeric(num['ddate'], errors='coerce')
    num['qtrs'] = pd.to_numeric(num['qtrs'], errors='coerce').fillna(-1).astype(int)
    num['value'] = pd.to_numeric(num['value'], errors='coerce')

    # Columnas que necesitamos de num
    num = num[['adsh', 'tag', 'ddate', 'qtrs', 'value']]

    # --- JOIN con sub para obtener 'period' ---
    merged = num.merge(sub[['adsh', 'period']], on='adsh', how='inner')

    if len(merged) == 0:
        return None

    # FILTRO 5: Solo datos del período actual (ddate == period)
    # Esto descarta datos comparativos de períodos anteriores
    merged = merged[merged['ddate'] == merged['period']]

    if len(merged) == 0:
        return None

    # FILTRO 6: Dedup — resolver múltiples valores por (adsh, tag)
    # Estrategia: tomar el valor con mayor qtrs (más comprensivo)
    # Para balance (qtrs=0): solo hay una opción, se mantiene
    # Para flujos: toma el acumulado más largo
    merged = merged.sort_values(
        ['adsh', 'tag', 'qtrs'],
        ascending=[True, True, False]
    )
    merged = merged.drop_duplicates(subset=['adsh', 'tag'], keep='first')

    # Retornar solo lo necesario para el pivot
    return merged[['adsh', 'tag', 'value', 'qtrs']]


def consolidar(directorio: Path, inicio: str, fin: str, salida: Path):
    """
    Proceso principal: lee todos los trimestres, consolida y genera CSV.
    """
    print("\n" + "═" * 75)
    print("  🔧 CONSOLIDADOR DE VARIABLES FINANCIERAS CRUDAS (SEC DERA)")
    print("═" * 75)
    print(f"  Tags XBRL incluidos: {len(ALL_TAGS)}")
    print(f"    → Monetarios (USD):  {len(TAGS_USD)}")
    print(f"    → Acciones (shares): {len(TAGS_SHARES)}")
    print(f"  Período: {inicio} → {fin}")
    print(f"  Directorio: {directorio.absolute()}")

    trimestres = generar_trimestres(inicio, fin)
    all_values = []
    all_subs = []

    print(f"\n  📂 Procesando {len(trimestres)} trimestres...\n")

    for anio, q in tqdm(trimestres, desc="  Trimestres"):
        dir_q = directorio / f"{anio}Q{q}"

        # Procesar datos numéricos
        result = procesar_trimestre(dir_q)
        if result is not None:
            all_values.append(result)

        # Leer metadata de sub.csv
        sub_path = dir_q / "sub.csv"
        if sub_path.exists():
            sub = pd.read_csv(sub_path, low_memory=False, dtype={'cik': str, 'sic': str})
            cols_disponibles = [c for c in SUB_COLS if c in sub.columns]
            sub = sub[cols_disponibles]
            sub = sub[sub['form'].isin(FORMS_VALID)]
            if len(sub) > 0:
                all_subs.append(sub)

    if not all_values:
        print("  ❌ No hay datos para consolidar")
        return

    # --- Combinar todos los trimestres ---
    print(f"\n  📦 Combinando datos...")
    values = pd.concat(all_values, ignore_index=True)
    subs = pd.concat(all_subs, ignore_index=True)

    # Dedup subs (mismo adsh puede estar en all_subs una sola vez, pero por seguridad)
    subs = subs.drop_duplicates(subset=['adsh'], keep='last')

    print(f"     Filings totales:    {len(subs):,}")
    print(f"     Data points crudos: {len(values):,}")

    # --- PIVOT: formato largo → ancho ---
    print("  🔄 Pivotando datos (largo → ancho)...")

    # Pivot de valores
    pivot_values = values.pivot_table(
        index='adsh',
        columns='tag',
        values='value',
        aggfunc='first'  # Ya no hay duplicados después de nuestros filtros
    )

    # Pivot de qtrs (para saber la cobertura temporal de cada valor)
    pivot_qtrs = values.pivot_table(
        index='adsh',
        columns='tag',
        values='qtrs',
        aggfunc='first'
    )
    # Renombrar columnas de qtrs con sufijo
    pivot_qtrs.columns = [f"{c}_qtrs" for c in pivot_qtrs.columns]

    # Combinar valores + qtrs
    pivot = pd.concat([pivot_values, pivot_qtrs], axis=1)
    pivot = pivot.reset_index()

    # --- JOIN con metadata de sub.csv ---
    print("  📋 Agregando metadata de empresas...")
    final = pivot.merge(subs, on='adsh', how='inner')

    # --- Ordenar columnas ---
    # 1. Metadata, 2. Tags de balance, 3. Tags de IS, 4. Tags de CF, 5. Tags de shares, 6. qtrs
    meta_cols = [c for c in SUB_COLS if c in final.columns]
    tag_value_cols = sorted([c for c in final.columns if c in ALL_TAGS])
    qtrs_cols = sorted([c for c in final.columns if c.endswith('_qtrs')])
    other_cols = [c for c in final.columns if c not in meta_cols + tag_value_cols + qtrs_cols]

    ordered_cols = meta_cols + tag_value_cols + qtrs_cols
    # Agregar cualquier columna que hayamos olvidado
    for c in final.columns:
        if c not in ordered_cols:
            ordered_cols.append(c)

    final = final[ordered_cols]

    # --- Reporte de cobertura por tag ---
    print("\n  📊 COBERTURA POR VARIABLE:")
    total_filings = len(final)
    cobertura = []
    for tag in sorted(ALL_TAGS):
        if tag in final.columns:
            n = final[tag].notna().sum()
            pct = n / total_filings * 100
            cobertura.append((tag, n, pct))
    
    cobertura.sort(key=lambda x: x[2], reverse=True)
    for tag, n, pct in cobertura[:20]:
        bar = "█" * int(pct / 5)
        print(f"     {tag:60s} {n:>8,} ({pct:5.1f}%) {bar}")
    if len(cobertura) > 20:
        print(f"     ... y {len(cobertura) - 20} variables más")

    # Sin cobertura
    tags_sin_datos = [t for t in ALL_TAGS if t not in final.columns or final[t].notna().sum() == 0]
    if tags_sin_datos:
        print(f"\n  ⚠  Tags sin datos: {', '.join(sorted(tags_sin_datos))}")

    # --- Guardar ---
    print(f"\n  💾 Guardando...")
    final.to_csv(salida, index=False)
    tamano_mb = salida.stat().st_size / (1024 * 1024)

    print(f"\n  {'═' * 65}")
    print(f"  ✅ DATASET CRUDO CONSOLIDADO GUARDADO")
    print(f"  {'═' * 65}")
    print(f"     📄 Archivo:    {salida}")
    print(f"     📊 Filas:      {len(final):,} filings")
    print(f"     📊 Columnas:   {len(final.columns)} ({len(tag_value_cols)} variables + {len(qtrs_cols)} qtrs + {len(meta_cols)} metadata)")
    print(f"     🏢 Empresas:   {final['cik'].nunique():,} únicas")
    if 'period' in final.columns:
        print(f"     📅 Período:    {final['period'].min()} → {final['period'].max()}")
    print(f"     💾 Tamaño:     {tamano_mb:.1f} MB")
    print(f"\n     ℹ  Este CSV contiene SOLO datos crudos.")
    print(f"        Ratios, scores y features derivadas se calculan aparte.")


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Consolida variables financieras crudas de SEC DERA en un solo CSV"
    )
    parser.add_argument('--inicio', default='2014Q1',
                        help='Trimestre inicial (default: 2014Q1)')
    parser.add_argument('--fin', default='2025Q4',
                        help='Trimestre final (default: 2025Q4)')
    parser.add_argument('--directorio', default='datos_sec_edgar',
                        help='Directorio con datos descargados')
    parser.add_argument('--salida', default=None,
                        help='Ruta del CSV de salida')

    args = parser.parse_args()
    directorio = Path(args.directorio)

    if args.salida:
        salida = Path(args.salida)
    else:
        salida = directorio / "VARIABLES_FINANCIERAS_CRUDAS.csv"

    if not directorio.exists():
        print(f"  ❌ Directorio no encontrado: {directorio}")
        sys.exit(1)

    consolidar(directorio, args.inicio, args.fin, salida)
    print("\n  ✅ ¡Proceso completado!\n")


if __name__ == "__main__":
    main()
