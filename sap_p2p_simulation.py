"""
================================================================================
SAP MM — Procure-to-Cash (P2P) Analytics Simulation
================================================================================

CONCEPTUAL OVERVIEW
-------------------
This script simulates the SAP MM (Materials Management) Procure-to-Pay process.
In a real SAP environment, procurement data is stored across several relational
tables linked by the Purchase Order number (EBELN):

  EKKO  →  Purchase Order Header      (one row per PO)
  EKPO  →  Purchase Order Item Data   (multiple items per PO)
  MIGO  →  Goods Receipt Document     (delivery confirmation)
  MIRO  →  Logistics Invoice          (vendor invoice & payment)

The pipeline mirrors the real-world P2P cycle:
  1. Purchase Requisition → Purchase Order (EKKO + EKPO)
  2. Goods Receipt        → Inventory Update (MIGO)
  3. Invoice Verification → Payment (MIRO)

We simulate ~300 purchase orders across 12 months (2024), then join them
using EBELN (PO Number) to replicate an SAP data extraction pipeline.
================================================================================
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
import warnings
warnings.filterwarnings('ignore')

np.random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: DATA GENERATION — Simulate SAP MM Tables
# ─────────────────────────────────────────────────────────────────────────────

N_PO = 300  # Number of Purchase Orders

# SAP Table: EKKO — Purchase Order Header
# Key fields: EBELN (PO#), LIFNR (Vendor), BEDAT (PO Date), EKGRP (Purch. Group)
vendors = {
    'V001': 'TechSource India',
    'V002': 'GlobalSupply Co.',
    'V003': 'RapidParts Ltd.',
    'V004': 'PrimeMaterials',
    'V005': 'SwiftLogistics'
}
po_dates = pd.date_range('2024-01-01', '2024-12-31', periods=N_PO)
po_dates = sorted(np.random.choice(po_dates, N_PO, replace=False))

EKKO = pd.DataFrame({
    'EBELN': [f'4500{str(i).zfill(6)}' for i in range(1, N_PO + 1)],
    'LIFNR': np.random.choice(list(vendors.keys()), N_PO, p=[0.30, 0.25, 0.20, 0.15, 0.10]),
    'BEDAT': po_dates,
    'EKGRP': np.random.choice(['EG1', 'EG2', 'EG3'], N_PO)
})

# SAP Table: EKPO — Purchase Order Item Data
# Key fields: EBELN (PO#), EBELP (Item#), MATNR (Material), NETWR (Net Value), MENGE (Qty)
materials = {
    'RAW-STEEL':   ('Raw Steel',    3000, 8000),
    'ELEC-COMP':   ('Electronics',  1500, 5000),
    'PACK-MAT':    ('Packaging',     500, 2000),
    'CHEM-SOLV':   ('Chemicals',    2000, 6000),
    'MECH-PART':   ('Mech Parts',   1000, 4000),
    'OFFICE-SUP':  ('Office Suppl',  200,  800),
    'IT-HARDWARE': ('IT Hardware',  4000,12000),
    'SAFETY-EQ':   ('Safety Equip',  800, 2500),
}

ekpo_rows = []
for _, row in EKKO.iterrows():
    n_items = np.random.randint(1, 5)
    selected = np.random.choice(list(materials.keys()), n_items, replace=False)
    for item_no, mat in enumerate(selected, 1):
        desc, lo, hi = materials[mat]
        qty = np.random.randint(5, 50)
        unit_price = np.random.uniform(lo, hi)
        ekpo_rows.append({
            'EBELN': row['EBELN'],
            'EBELP': str(item_no * 10).zfill(5),
            'MATNR': mat,
            'TXZ01': desc,
            'MENGE': qty,
            'NETPR': round(unit_price, 2),
            'NETWR': round(qty * unit_price, 2)
        })

EKPO = pd.DataFrame(ekpo_rows)

# SAP Table: MIGO — Goods Receipt
# Key fields: EBELN (PO#), WADAT_IST (Actual GR Date), MBLNR (Material Doc#)
# Simulates delivery lag: 3–20 days after PO date
gr_rows = []
ekko_date_map = dict(zip(EKKO['EBELN'], EKKO['BEDAT']))
for ebeln in EKKO['EBELN']:
    if np.random.rand() > 0.05:  # 5% POs have no GR yet (open)
        po_date = ekko_date_map[ebeln]
        pool = np.concatenate([
            np.random.normal(7, 2, 60).astype(int),
            np.random.normal(15, 3, 30).astype(int),
            np.random.normal(22, 2, 10).astype(int)
        ])
        lead_days = int(np.random.choice(pool))
        lead_days = max(2, min(lead_days, 30))
        gr_date = po_date + pd.Timedelta(days=lead_days)
        gr_rows.append({
            'EBELN': ebeln,
            'MBLNR': f'5000{np.random.randint(100000, 999999)}',
            'WADAT_IST': gr_date,
            'GR_LEAD_DAYS': lead_days
        })

MIGO = pd.DataFrame(gr_rows)

# SAP Table: MIRO — Logistics Invoice Verification
# Key fields: EBELN (PO#), BLDAT (Invoice Date), BELNR (Accounting Doc#)
# Invoice arrives 1–7 days after GR
gr_date_map = dict(zip(MIGO['EBELN'], MIGO['WADAT_IST']))
miro_rows = []
for ebeln in MIGO['EBELN']:
    if np.random.rand() > 0.08:  # 8% GRs not yet invoiced
        gr_date = gr_date_map[ebeln]
        inv_delay = np.random.randint(1, 8)
        miro_rows.append({
            'EBELN': ebeln,
            'BELNR': f'9100{np.random.randint(100000, 999999)}',
            'BLDAT': gr_date + pd.Timedelta(days=inv_delay),
            'PAYMENT_STATUS': np.random.choice(
                ['Paid', 'Pending', 'Overdue'],
                p=[0.65, 0.25, 0.10]
            )
        })

MIRO = pd.DataFrame(miro_rows)

print("=== SAP MM Table Row Counts ===")
print(f"EKKO (PO Header):     {len(EKKO):>5} rows")
print(f"EKPO (PO Items):      {len(EKPO):>5} rows")
print(f"MIGO (Goods Receipt): {len(MIGO):>5} rows")
print(f"MIRO (Invoice):       {len(MIRO):>5} rows")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2: DATA PROCESSING — Join Tables (Simulating SAP Pipeline)
# ─────────────────────────────────────────────────────────────────────────────
# Join order mirrors an SAP BW extraction:
#   EKKO ──EBELN──► EKPO ──EBELN──► MIGO ──EBELN──► MIRO
# ─────────────────────────────────────────────────────────────────────────────

merged = (
    EKPO
    .merge(EKKO[['EBELN', 'LIFNR', 'BEDAT', 'EKGRP']], on='EBELN', how='left')
    .merge(MIGO[['EBELN', 'WADAT_IST', 'GR_LEAD_DAYS']], on='EBELN', how='left')
    .merge(MIRO[['EBELN', 'BLDAT', 'PAYMENT_STATUS']], on='EBELN', how='left')
)
merged['VENDOR_NAME'] = merged['LIFNR'].map(vendors)
merged['BEDAT'] = pd.to_datetime(merged['BEDAT'])
merged['MONTH'] = merged['BEDAT'].dt.to_period('M')

print(f"\nMerged pipeline rows: {len(merged)}")
print(f"Total Procurement Spend: ₹{merged['NETWR'].sum()/1e6:.2f}M")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3: VISUALIZATIONS
# ─────────────────────────────────────────────────────────────────────────────

BG    = '#0d1117'
CARD  = '#161b22'
GRID  = '#21262d'
TEXT  = '#e6edf3'
MUTED = '#8b949e'
COLORS = ['#58a6ff','#f78166','#3fb950','#d2a8ff','#ffa657','#79c0ff','#ff7b72','#56d364']

plt.rcParams.update({
    'figure.facecolor': BG,
    'axes.facecolor':   CARD,
    'axes.edgecolor':   GRID,
    'axes.labelcolor':  TEXT,
    'xtick.color':      MUTED,
    'ytick.color':      MUTED,
    'text.color':       TEXT,
    'grid.color':       GRID,
    'grid.linewidth':   0.6,
    'font.family':      'monospace',
})

fig = plt.figure(figsize=(18, 22), facecolor=BG)
fig.suptitle('SAP MM — Procure-to-Pay Analytics Dashboard',
             fontsize=18, fontweight='bold', color=TEXT, y=0.98,
             fontfamily='monospace')

# ── CHART 1: Spend by Material (EKPO.NETWR grouped by MATNR) ──────────────
ax1 = fig.add_subplot(3, 2, (1, 2))
spend_mat = (merged.groupby('TXZ01')['NETWR']
             .sum().sort_values(ascending=True) / 1e6)
bars = ax1.barh(spend_mat.index, spend_mat.values,
                color=COLORS[:len(spend_mat)], height=0.65, edgecolor='none')
for bar, val in zip(bars, spend_mat.values):
    ax1.text(val + 0.3, bar.get_y() + bar.get_height()/2,
             f'₹{val:.2f}M', va='center', fontsize=9, color=TEXT)
ax1.set_xlabel('Procurement Spend (₹ Millions)', color=MUTED, fontsize=10)
ax1.set_title('Total Spend by Material Category  (EKPO.NETWR)',
              fontsize=12, fontweight='bold', color=TEXT, pad=12)
ax1.xaxis.grid(True, alpha=0.4)
ax1.set_axisbelow(True)
ax1.spines[['top','right','left']].set_visible(False)

# ── CHART 2: PO-to-GR Lead Time Distribution ─────────────────────────────
ax2 = fig.add_subplot(3, 2, 3)
lead_data = merged.dropna(subset=['GR_LEAD_DAYS'])['GR_LEAD_DAYS'].astype(int)
mean_lt  = lead_data.mean()
sla_days = 10
warn_days= 15

bins = range(1, 32)
counts, edges = np.histogram(lead_data, bins=bins)
bar_colors = []
for e in edges[:-1]:
    if e <= sla_days:
        bar_colors.append('#3fb950')
    elif e <= warn_days:
        bar_colors.append('#d29922')
    else:
        bar_colors.append('#f78166')

ax2.bar(edges[:-1], counts, width=0.85, color=bar_colors, edgecolor='none', align='edge')
ax2.axvline(mean_lt,  color='white',   linestyle='--', lw=1.5, label=f'Mean: {mean_lt:.1f}d')
ax2.axvline(sla_days, color='#3fb950', linestyle=':',  lw=1.5, label=f'SLA: {sla_days}d')
ax2.axvline(warn_days,color='#d29922', linestyle=':',  lw=1.5, label=f'Warning: {warn_days}d')
ax2.set_xlabel('Lead Time (Days from PO to Goods Receipt)', color=MUTED, fontsize=9)
ax2.set_ylabel('Number of POs', color=MUTED, fontsize=9)
ax2.set_title('PO-to-Goods Receipt Lead Time\n(EKKO.BEDAT → MIGO.WADAT_IST)',
              fontsize=11, fontweight='bold', color=TEXT, pad=10)
ax2.legend(fontsize=8, facecolor=CARD, edgecolor=GRID, labelcolor=TEXT)
ax2.xaxis.grid(True, alpha=0.3); ax2.yaxis.grid(True, alpha=0.3)
ax2.set_axisbelow(True)
ax2.spines[['top','right']].set_visible(False)

# ── CHART 3: Spend by Vendor ──────────────────────────────────────────────
ax3 = fig.add_subplot(3, 2, 4)
spend_vendor = (merged.groupby('VENDOR_NAME')['NETWR']
                .sum().sort_values(ascending=False) / 1e6)
wedge_colors = COLORS[:len(spend_vendor)]
wedges, texts, autotexts = ax3.pie(
    spend_vendor.values,
    labels=None,
    autopct='%1.1f%%',
    colors=wedge_colors,
    startangle=140,
    pctdistance=0.75,
    wedgeprops=dict(edgecolor=BG, linewidth=2)
)
for at in autotexts:
    at.set_fontsize(8); at.set_color(BG); at.set_fontweight('bold')
ax3.legend(
    [f'{v}  ₹{s:.1f}M' for v, s in zip(spend_vendor.index, spend_vendor.values)],
    loc='lower left', fontsize=7.5, facecolor=CARD, edgecolor=GRID, labelcolor=TEXT,
    bbox_to_anchor=(-0.15, -0.15)
)
ax3.set_title('Vendor-wise Procurement Spend\n(EKKO.LIFNR)',
              fontsize=11, fontweight='bold', color=TEXT, pad=10)

# ── CHART 4: Monthly Procurement Trend ───────────────────────────────────
ax4  = fig.add_subplot(3, 1, 3)
ax4b = ax4.twinx()

monthly = (merged.groupby('MONTH').agg(
    Spend=('NETWR', 'sum'),
    PO_Count=('EBELN', 'nunique')
).reset_index())
monthly['Spend_M'] = monthly['Spend'] / 1e6
monthly['Month_str'] = monthly['MONTH'].astype(str)
monthly['Rolling3'] = monthly['Spend_M'].rolling(3, min_periods=1).mean()

x = range(len(monthly))
ax4b.bar(x, monthly['PO_Count'], color='#1f6feb', alpha=0.35, label='PO Volume', zorder=1)
ax4.plot(x, monthly['Spend_M'], color='#58a6ff', marker='o', ms=6,
         lw=2.2, label='Spend (₹M)', zorder=3)
ax4.plot(x, monthly['Rolling3'], color='#f78166', linestyle='--',
         lw=1.8, label='3M Rolling Avg', zorder=2)

ax4.set_xticks(list(x))
ax4.set_xticklabels(monthly['Month_str'], rotation=35, ha='right', fontsize=8)
ax4.set_ylabel('Procurement Spend (₹M)', color=MUTED, fontsize=9)
ax4b.set_ylabel('PO Count', color='#1f6feb', fontsize=9)
ax4b.tick_params(axis='y', labelcolor='#1f6feb')

ax4.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f'₹{v:.1f}M'))
ax4.set_title('Monthly Procurement Trend — Spend & PO Volume (2024)\nSource: EKKO.BEDAT + EKPO.NETWR',
              fontsize=11, fontweight='bold', color=TEXT, pad=10)

lines1, labels1 = ax4.get_legend_handles_labels()
bar_patch = Line2D([0],[0], color='#1f6feb', lw=8, alpha=0.35, label='PO Volume')
ax4.legend(handles=lines1+[bar_patch], fontsize=9,
           facecolor=CARD, edgecolor=GRID, labelcolor=TEXT, loc='upper left')

ax4.xaxis.grid(True, alpha=0.3); ax4.yaxis.grid(True, alpha=0.3)
ax4.set_axisbelow(True)
ax4.spines[['top','right']].set_visible(False)
ax4b.spines[['top']].set_visible(False)

# Footer
fig.text(0.5, 0.01,
         'Simulated SAP MM Tables: EKKO · EKPO · MIGO · MIRO   |   P2P Pipeline Simulation   |   All values in INR',
         ha='center', fontsize=8, color=MUTED)

plt.tight_layout(rect=[0, 0.02, 1, 0.97])
plt.savefig('/home/claude/sap_p2p_dashboard.png', dpi=150, bbox_inches='tight',
            facecolor=BG, edgecolor='none')
print("\nDashboard saved: sap_p2p_dashboard.png")

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY STATISTICS
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== P2P Pipeline Summary ===")
print(f"Total POs Generated:      {len(EKKO)}")
print(f"Total Line Items:         {len(EKPO)}")
print(f"Total Spend:              ₹{merged['NETWR'].sum()/1e6:.2f}M")
print(f"Avg PO Value:             ₹{merged.groupby('EBELN')['NETWR'].sum().mean()/1000:.1f}K")
print(f"Avg GR Lead Time:         {MIGO['GR_LEAD_DAYS'].mean():.1f} days")
print(f"POs Within SLA (≤10d):    {(MIGO['GR_LEAD_DAYS'] <= 10).mean()*100:.1f}%")
print(f"Overdue Invoices:         {(MIRO['PAYMENT_STATUS']=='Overdue').sum()}")
