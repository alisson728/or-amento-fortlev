from flask import Flask, render_template, request, redirect, url_for, abort, session, flash, send_file
import os
import shutil
import sqlite3
import json
import psycopg2
from psycopg2.extras import DictCursor
import io
import math
import re
from datetime import datetime
from pathlib import Path
from functools import wraps
from types import SimpleNamespace

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import simpleSplit
from reportlab.graphics.shapes import Drawing, String
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics import renderPDF
from reportlab.pdfgen import canvas

app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get('DATA_DIR', str(BASE_DIR)))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_DB_PATH = BASE_DIR / 'database.db'
DB_PATH = Path(os.environ.get('DATABASE_PATH', str(DATA_DIR / 'database.db')))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
if DB_PATH != DEFAULT_DB_PATH and not DB_PATH.exists() and DEFAULT_DB_PATH.exists():
    shutil.copy2(DEFAULT_DB_PATH, DB_PATH)
DATABASE_URL = (os.environ.get('DATABASE_URL') or '').strip()

app.secret_key = os.environ.get('SECRET_KEY', 'fortlev-secret')
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
)


DEFAULT_GROUPS = [
    {
        "nome": "Módulos Fotovoltaicos",
        "subgrupos": [
            {"nome": "Módulos 550W", "itens": [
                {"nome": "Módulo 550W Mono PERC", "valor": 640.0},
                {"nome": "Módulo 555W Half Cell", "valor": 655.0}
            ]},
            {"nome": "Módulos Bifaciais", "itens": [
                {"nome": "Módulo 570W Bifacial", "valor": 720.0},
                {"nome": "Módulo 580W Bifacial", "valor": 745.0}
            ]}
        ]
    },
    {
        "nome": "Inversores",
        "subgrupos": [
            {"nome": "String", "itens": [
                {"nome": "Inversor 50 kW", "valor": 18500.0},
                {"nome": "Inversor 100 kW", "valor": 34900.0}
            ]},
            {"nome": "Acessórios", "itens": [
                {"nome": "String Box", "valor": 1850.0},
                {"nome": "Monitoramento do Inversor", "valor": 650.0}
            ]}
        ]
    },
    {
        "nome": "Estruturas",
        "subgrupos": [
            {"nome": "Solo", "itens": [
                {"nome": "Estrutura Solo Galvanizada", "valor": 130.0},
                {"nome": "Fixadores e Ferragens", "valor": 18.0}
            ]},
            {"nome": "Telhado", "itens": [
                {"nome": "Estrutura Telhado Cerâmico", "valor": 120.0},
                {"nome": "Estrutura Telhado Metálico", "valor": 110.0}
            ]}
        ]
    },
]

UFV_TYPE_FIXED_ITEMS = {
    'UFV SOLO': [
        "Rede de Média MT", "Operação e Manutenção", "SCADA/Licenças", "Vigilância / Monitoramento",
        "Mão de obra Civil", "Seguro Instalação", "Projetos Civil/Elétrico", "Mão de obra Elétrica",
        "Comissionamento/Comunicação", "Adequação SMF", "Laudo Estrutural", "Acompanhamento de obra",
        "Sondagem SPT", "Topografia"
    ],
    'UFV CARPORT': [
        "Rede de Média MT", "Operação e Manutenção", "SCADA/Licenças", "Vigilância / Monitoramento",
        "Mão de obra Civil", "Seguro Instalação", "Projetos Civil/Elétrico", "Mão de obra Elétrica",
        "Comissionamento/Comunicação", "Adequação SMF", "Laudo Estrutural", "Acompanhamento de obra",
        "Sondagem SPT", "Topografia"
    ],
    'UFV TELHADO': [
        "Rede de Média MT", "Operação e Manutenção", "SCADA/Licenças", "Vigilância / Monitoramento",
        "Mão de obra Civil", "Seguro Instalação", "Projetos Civil/Elétrico", "Mão de obra Elétrica",
        "Comissionamento/Comunicação", "Adequação SMF", "Acompanhamento de obra"
    ],
    'UFV FLUTUANTE': [
        "Rede de Média MT", "Operação e Manutenção", "SCADA/Licenças", "Vigilância / Monitoramento",
        "Mão de obra Civil", "Seguro Instalação", "Projetos Civil/Elétrico", "Mão de obra Elétrica",
        "Comissionamento/Comunicação", "Adequação SMF", "Batimetria", "Acompanhamento de obra",
        "Ancoragem (Material + M.O)", "Topografia"
    ],
}
FIXED_ITEMS = UFV_TYPE_FIXED_ITEMS['UFV SOLO']
UFV_TYPES = list(UFV_TYPE_FIXED_ITEMS.keys())

PDF_TEMPLATE_DIR = BASE_DIR / 'assets' / 'pdf_template'
PDF_LOGO_PATH = PDF_TEMPLATE_DIR / 'fortlev_logo.png'
PDF_ORANGE_BAR_PATH = PDF_TEMPLATE_DIR / 'orange_bar.png'
PDF_BLUE = colors.HexColor('#4B74C2')
PDF_DARK_BLUE = colors.HexColor('#1A3E73')
PDF_ORANGE = colors.HexColor('#F58220')
PDF_TEXT = colors.black
INTRO_TEXT = [
    'Esta proposta tem como objetivo apresentar o levantamento de materiais e serviços necessários para a implantação de usinas fotovoltaicas no modelo EPC (Engineering, Procurement and Construction), considerando o formato Turnkey.',
    'No modelo Turnkey, o projeto é entregue completo e pronto para operação, sendo de responsabilidade da empresa contratada todas as etapas envolvidas, desde o planejamento, aquisição de equipamentos, execução das obras até o comissionamento final do sistema. Esse formato proporciona maior segurança, padronização e praticidade ao cliente, que recebe a usina em pleno funcionamento, sem a necessidade de gerenciamento direto das fases do projeto.',
    'Dessa forma, esta proposta contempla exclusivamente os itens relacionados ao fornecimento e execução necessários para a entrega da usina fotovoltaica, garantindo qualidade, desempenho e conformidade com as normas técnicas aplicáveis.',
]


def format_number_br(value, decimals=2):
    try:
        num = float(value or 0)
    except (TypeError, ValueError):
        num = 0.0
    fmt = f"{num:,.{decimals}f}"
    return fmt.replace(',', 'X').replace('.', ',').replace('X', '.')


def format_brl_pdf(value, decimals=2):
    return f"R$ {format_number_br(value, decimals)}"


def safe_filename(value):
    cleaned = re.sub(r'[^A-Za-z0-9._-]+', '_', (value or '').strip())
    return cleaned.strip('._') or 'proposta'


def pdf_header(canvas_obj, page_width, page_height):
    if PDF_LOGO_PATH.exists():
        canvas_obj.drawImage(str(PDF_LOGO_PATH), 42, page_height - 92, width=150, height=54, mask='auto')
    canvas_obj.setFillColor(PDF_TEXT)
    canvas_obj.setFont('Helvetica-Bold', 14)
    canvas_obj.drawRightString(page_width - 42, page_height - 42, 'FORTLEV ENERGIA SOLAR LTDA')
    canvas_obj.setFont('Helvetica', 10.5)
    canvas_obj.drawRightString(page_width - 42, page_height - 58, 'CNPJ: 32.270.838/0002-72')
    canvas_obj.drawRightString(page_width - 42, page_height - 72, 'Av. Civit nº 22, Galpão 1, Civit I')
    canvas_obj.drawRightString(page_width - 42, page_height - 86, '29.168-045 Serra/ES')


def pdf_section_title(canvas_obj, x, y, title, line_width=230):
    canvas_obj.setFillColor(PDF_DARK_BLUE)
    canvas_obj.setFont('Helvetica-Bold', 20)
    canvas_obj.drawString(x, y, title)
    if PDF_ORANGE_BAR_PATH.exists():
        canvas_obj.drawImage(str(PDF_ORANGE_BAR_PATH), x, y - 18, width=line_width, height=12, mask='auto')
    else:
        canvas_obj.setFillColor(PDF_ORANGE)
        canvas_obj.rect(x, y - 18, line_width, 8, fill=1, stroke=0)
    canvas_obj.setFillColor(PDF_TEXT)


def draw_wrapped_paragraph(canvas_obj, text, x, y, width, font_name='Times-Roman', font_size=13, leading=19):
    lines = simpleSplit(text, font_name, font_size, width)
    text_obj = canvas_obj.beginText(x, y)
    text_obj.setFont(font_name, font_size)
    text_obj.setLeading(leading)
    for line in lines:
        text_obj.textLine(line)
    canvas_obj.drawText(text_obj)
    return y - (len(lines) * leading)


def draw_table_header(canvas_obj, x, y, widths, headers, row_h=24):
    cx = x
    canvas_obj.setStrokeColor(colors.black)
    canvas_obj.setFillColor(colors.HexColor('#8B8B8B'))
    for w, label in zip(widths, headers):
        canvas_obj.rect(cx, y - row_h, w, row_h, fill=1, stroke=1)
        canvas_obj.setFillColor(colors.white)
        canvas_obj.setFont('Helvetica-Bold', 10)
        canvas_obj.drawCentredString(cx + w / 2, y - 16, label)
        canvas_obj.setFillColor(colors.HexColor('#8B8B8B'))
        cx += w
    canvas_obj.setFillColor(PDF_TEXT)
    return y - row_h


def compute_table_row_height(values, widths, font_name='Helvetica', font_size=9.4, padding=4, leading=12):
    max_lines = 1
    for value, width in zip(values, widths):
        content = str(value if value is not None else '')
        lines = simpleSplit(content, font_name, font_size, max(width - padding * 2, 24))
        max_lines = max(max_lines, len(lines))
    return max(22, padding * 2 + max_lines * leading)


def draw_table_row(canvas_obj, x, y, widths, values, aligns=None, font_name='Helvetica', font_size=9.4, padding=4, leading=12):
    row_h = compute_table_row_height(values, widths, font_name, font_size, padding, leading)
    cx = x
    aligns = aligns or ['left'] * len(values)
    canvas_obj.setStrokeColor(colors.black)
    canvas_obj.setFillColor(colors.white)
    for idx, (w, value) in enumerate(zip(widths, values)):
        canvas_obj.setFillColor(colors.white)
        canvas_obj.rect(cx, y - row_h, w, row_h, fill=1, stroke=1)
        lines = simpleSplit(str(value if value is not None else ''), font_name, font_size, max(w - padding * 2, 24))
        text_y = y - padding - font_size
        canvas_obj.setFillColor(PDF_TEXT)
        canvas_obj.setFont(font_name, font_size)
        for line in lines:
            if aligns[idx] == 'right':
                canvas_obj.drawRightString(cx + w - padding, text_y, line)
            elif aligns[idx] == 'center':
                canvas_obj.drawCentredString(cx + w / 2, text_y, line)
            else:
                canvas_obj.drawString(cx + padding, text_y, line)
            text_y -= leading
        cx += w
    return y - row_h


def build_export_context(orcamento_id):
    conn = get_db()
    row = conn.execute('SELECT o.*, c.nome as cliente_nome FROM orcamentos o LEFT JOIN clientes c ON c.id=o.cliente_id WHERE o.id = ?', (orcamento_id,)).fetchone()
    conn.close()
    if not row:
        return None
    try:
        payload = json.loads(row['data_json'] or '{}')
    except json.JSONDecodeError:
        payload = {}

    potencia_wp = float(row['potencia'] or 0) * 1000
    material_groups = []
    grouped = {}
    for item in payload.get('materiais', []) or []:
        total = safe_float(item.get('total')) or (safe_float(item.get('qtd'), 0) * safe_float(item.get('valor'), 0))
        grupo = (item.get('grupo') or 'Sem grupo').strip() or 'Sem grupo'
        normalized = {
            'grupo': grupo,
            'subgrupo': item.get('subgrupo') or '-',
            'item': item.get('item') or '-',
            'qtd': safe_float(item.get('qtd'), 0),
            'valor': safe_float(item.get('valor'), 0),
            'total': total,
            'rwp': (total / potencia_wp) if potencia_wp > 0 else 0,
        }
        grouped.setdefault(grupo, []).append(normalized)

    for grupo_nome, rows in grouped.items():
        total_grupo = sum(r['total'] for r in rows)
        material_groups.append({
            'grupo': grupo_nome,
            'rows': rows,
            'total': total_grupo,
            'rwp': (total_grupo / potencia_wp) if potencia_wp > 0 else 0,
        })
    material_groups.sort(key=lambda g: g['grupo'].lower())

    fixed_rows = []
    for item in payload.get('fixos', []) or []:
        valor = safe_float(item.get('valor'), 0)
        fixed_rows.append({'nome': item.get('nome') or '-', 'valor': valor})

    kit_rows = []
    for item in payload.get('kit_fotovoltaico', []) or []:
        valor = safe_float(item.get('valor'), 0)
        kit_rows.append({'nome': item.get('nome') or '-', 'valor': valor})

    return {
        'orcamento': row,
        'payload': payload,
        'potencia_wp': potencia_wp,
        'material_groups': material_groups,
        'fixed_rows': fixed_rows,
        'kit_rows': kit_rows,
    }


def create_group_chart(groups):
    labels = [g['grupo'] for g in groups] or ['Sem grupos']
    values = [max(0.0, float(g['total'])) for g in groups] or [0.0]
    drawing = Drawing(470, 230)
    chart = VerticalBarChart()
    chart.x = 45
    chart.y = 50
    chart.height = 145
    chart.width = 385
    chart.data = [values]
    chart.strokeColor = colors.HexColor('#415A8B')
    chart.valueAxis.valueMin = 0
    max_value = max(values) if values else 0
    chart.valueAxis.valueMax = max_value * 1.15 if max_value > 0 else 1
    chart.valueAxis.valueStep = max(1, math.ceil((chart.valueAxis.valueMax or 1) / 5))
    chart.valueAxis.labels.fontName = 'Helvetica'
    chart.valueAxis.labels.fontSize = 8
    chart.categoryAxis.categoryNames = labels
    chart.categoryAxis.labels.boxAnchor = 'ne'
    chart.categoryAxis.labels.angle = 30
    chart.categoryAxis.labels.fontName = 'Helvetica'
    chart.categoryAxis.labels.fontSize = 8
    chart.categoryAxis.labels.dy = -2
    chart.categoryAxis.labels.dx = -6
    chart.categoryAxis.strokeColor = colors.HexColor('#666666')
    chart.categoryAxis.tickDown = 2
    chart.bars[0].fillColor = PDF_BLUE
    chart.bars[0].strokeColor = PDF_DARK_BLUE
    drawing.add(chart)
    drawing.add(String(235, 214, 'Totais por grupo', fontName='Helvetica-Bold', fontSize=12, textAnchor='middle', fillColor=PDF_DARK_BLUE))
    return drawing


def generate_orcamento_pdf(export_ctx):
    orcamento = export_ctx['orcamento']
    payload = export_ctx['payload']
    material_groups = export_ctx['material_groups']
    fixed_rows = export_ctx['fixed_rows']
    kit_rows = export_ctx['kit_rows']
    potencia_wp = export_ctx['potencia_wp']

    dados = payload.get('dados_principais', {}) or {}
    buffer = io.BytesIO()
    pdf_canvas = canvas.Canvas(buffer, pagesize=A4)
    page_w, page_h = A4

    pdf_header(pdf_canvas, page_w, page_h)
    cover_x = 28
    cover_y = page_h - 330
    cover_w = page_w - 56
    cover_h = 220
    pdf_canvas.setFillColor(PDF_BLUE)
    pdf_canvas.rect(cover_x, cover_y, cover_w, cover_h, fill=1, stroke=0)
    pdf_canvas.setFillColor(colors.white)
    pdf_canvas.setFont('Helvetica-Bold', 26)
    pdf_canvas.drawCentredString(page_w / 2, cover_y + 136, 'LEVANTAMENTO - SISTEMA')
    pdf_canvas.drawCentredString(page_w / 2, cover_y + 102, 'FOTOVOLTAICO')
    label_text = 'Levantamento de Proposta:'
    label_x = 72
    label_y = cover_y + 40
    pdf_canvas.setFillColor(colors.white)
    pdf_canvas.setFont('Helvetica-Bold', 14)
    pdf_canvas.drawString(label_x, label_y, label_text)
    value_x = label_x + pdf_canvas.stringWidth(label_text, 'Helvetica-Bold', 14) + 10
    pdf_canvas.setFillColor(colors.white)
    pdf_canvas.setFont('Helvetica-Bold', 14)
    pdf_canvas.drawString(value_x, label_y, str(orcamento['created_by'] or '-'))

    pdf_section_title(pdf_canvas, 72, page_h - 420, 'Dados do Projeto', line_width=305)
    details = [
        ('Projeto:', orcamento['nome'] or '-'),
        ('cliente:', orcamento['cliente_nome'] or '-'),
        ('modelo de usina:', dados.get('ufv_tipo') or '-'),
        ('Potência da Usina:', f"{format_number_br(orcamento['potencia'], 2)} kWp"),
        ('Potência do Módulo:', f"{format_number_br(dados.get('potencia_modulo'), 2)} Wp"),
        ('Quantidade de Módulos:', format_number_br(dados.get('quantidade_modulos'), 0)),
        ('Potência do Inversor:', f"{format_number_br(dados.get('potencia_inversor_kw'), 2)} kW"),
        ('Quantidade de Inversores:', format_number_br(dados.get('quantidade_inversores'), 0)),
    ]
    y = page_h - 470
    pdf_canvas.setFont('Times-Roman', 15)
    for label, value in details:
        pdf_canvas.setFillColor(PDF_TEXT)
        pdf_canvas.drawString(78, y, label)
        pdf_canvas.drawString(235, y, str(value))
        y -= 24
    pdf_canvas.showPage()

    pdf_header(pdf_canvas, page_w, page_h)
    pdf_section_title(pdf_canvas, 72, page_h - 145, 'Introdução', line_width=305)
    y = page_h - 200
    for paragraph in INTRO_TEXT:
        y = draw_wrapped_paragraph(pdf_canvas, paragraph, 58, y, page_w - 116, font_name='Times-Roman', font_size=13, leading=19) - 26
    pdf_canvas.showPage()

    def draw_material_page_header(title='Lista de Materiais'):
        pdf_header(pdf_canvas, page_w, page_h)
        pdf_section_title(pdf_canvas, 72, page_h - 150, title, line_width=305)
        return page_h - 205

    y = draw_material_page_header()
    table_x = 36
    widths = [220, 60, 88, 116, 70]
    headers = ['Item', 'Qtd', 'Valor Unit.', 'Total', 'R$/WP']
    for group in material_groups:
        estimated_height = 30 + 24 + sum(compute_table_row_height([r['item'], format_number_br(r['qtd'], 0), format_brl_pdf(r['valor']), format_brl_pdf(r['total']), format_brl_pdf(r['rwp'], 4)], widths) for r in group['rows']) + compute_table_row_height(['Total do Grupo', '', '', format_brl_pdf(group['total']), format_brl_pdf(group['rwp'], 4)], widths)
        if y - estimated_height < 80:
            pdf_canvas.showPage()
            y = draw_material_page_header()
        pdf_canvas.setFillColor(PDF_TEXT)
        pdf_canvas.setFont('Helvetica-Bold', 14)
        pdf_canvas.drawString(72, y, group['grupo'])
        y -= 18
        y = draw_table_header(pdf_canvas, table_x, y, widths, headers, row_h=24)
        for row in group['rows']:
            y = draw_table_row(pdf_canvas, table_x, y, widths, [row['item'], format_number_br(row['qtd'], 0), format_brl_pdf(row['valor']), format_brl_pdf(row['total']), format_brl_pdf(row['rwp'], 4)], aligns=['left', 'center', 'right', 'right', 'right'])
        y = draw_table_row(pdf_canvas, table_x, y, widths, ['Total do Grupo', '', '', format_brl_pdf(group['total']), format_brl_pdf(group['rwp'], 4)], aligns=['left', 'center', 'right', 'right', 'right'], font_name='Helvetica-Bold')
        y -= 18

    extra_sections = []
    if kit_rows:
        extra_sections.append(('Kit Fotovoltaico', [{'item': r['nome'], 'qtd': '-', 'valor': '', 'total': r['valor'], 'rwp': (r['valor'] / potencia_wp) if potencia_wp > 0 else 0} for r in kit_rows], sum(r['valor'] for r in kit_rows)))
    if fixed_rows:
        extra_sections.append(('Itens Fixos', [{'item': r['nome'], 'qtd': '-', 'valor': '', 'total': r['valor'], 'rwp': (r['valor'] / potencia_wp) if potencia_wp > 0 else 0} for r in fixed_rows], sum(r['valor'] for r in fixed_rows)))

    for section_title, rows, total_value in extra_sections:
        estimated_height = 30 + 24 + sum(compute_table_row_height([r['item'], r['qtd'], r['valor'], format_brl_pdf(r['total']), format_brl_pdf(r['rwp'], 4)], widths) for r in rows) + compute_table_row_height(['Total', '', '', format_brl_pdf(total_value), format_brl_pdf((total_value / potencia_wp) if potencia_wp > 0 else 0, 4)], widths)
        if y - estimated_height < 80:
            pdf_canvas.showPage()
            y = draw_material_page_header()
        pdf_canvas.setFillColor(PDF_DARK_BLUE)
        pdf_canvas.setFont('Helvetica-Bold', 14)
        pdf_canvas.drawString(72, y, section_title)
        y -= 18
        y = draw_table_header(pdf_canvas, table_x, y, widths, headers, row_h=24)
        for row in rows:
            y = draw_table_row(pdf_canvas, table_x, y, widths, [row['item'], row['qtd'], row['valor'], format_brl_pdf(row['total']), format_brl_pdf(row['rwp'], 4)], aligns=['left', 'center', 'right', 'right', 'right'])
        y = draw_table_row(pdf_canvas, table_x, y, widths, ['Total', '', '', format_brl_pdf(total_value), format_brl_pdf((total_value / potencia_wp) if potencia_wp > 0 else 0, 4)], aligns=['left', 'center', 'right', 'right', 'right'], font_name='Helvetica-Bold')
        y -= 18

    pdf_canvas.showPage()
    pdf_header(pdf_canvas, page_w, page_h)
    pdf_section_title(pdf_canvas, 72, page_h - 150, 'Resumo por Grupo', line_width=305)
    chart = create_group_chart(material_groups)
    renderPDF.draw(chart, pdf_canvas, 60, page_h - 430)
    legend_y = page_h - 470
    pdf_canvas.setFont('Helvetica', 10.5)
    pdf_canvas.setFillColor(PDF_TEXT)
    if material_groups:
        for idx, group in enumerate(material_groups[:10]):
            pdf_canvas.drawString(72, legend_y - idx * 16, f"{group['grupo']}: {format_brl_pdf(group['total'])}")
    else:
        pdf_canvas.drawString(72, legend_y, 'Nenhum grupo de materiais informado na proposta.')

    pdf_canvas.save()
    buffer.seek(0)
    return buffer



class PostgresCompatCursor:
    def __init__(self, cursor):
        self._cursor = cursor
        self.lastrowid = None

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def __iter__(self):
        return iter(self._cursor)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


def normalized_database_url(dsn):
    if not dsn:
        return dsn
    if dsn.startswith('postgres://'):
        dsn = 'postgresql://' + dsn[len('postgres://'):]
    if 'sslmode=' not in dsn and 'supabase.com' in dsn:
        sep = '&' if '?' in dsn else '?'
        dsn = f'{dsn}{sep}sslmode=require'
    return dsn


class PostgresCompatConnection:
    def __init__(self, dsn):
        self._raw = psycopg2.connect(normalized_database_url(dsn))

    def execute(self, query, params=None):
        cur = self._raw.cursor(cursor_factory=DictCursor)
        cur.execute(query.replace('?', '%s'), params or ())
        return PostgresCompatCursor(cur)

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        self._raw.close()


def get_db():
    if DATABASE_URL:
        return PostgresCompatConnection(DATABASE_URL)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn, table, column, ddl):
    if DATABASE_URL:
        conn.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {ddl}')
        return
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def now_str():
    return datetime.now().strftime('%d/%m/%Y %H:%M')


def safe_float(v, default=0.0):
    try:
        if v is None or v == '':
            return default
        return float(str(v).replace(',', '.'))
    except Exception:
        return default


def migrate_sqlite_to_postgres(conn):
    if not DEFAULT_DB_PATH.exists():
        return
    try:
        src = sqlite3.connect(DEFAULT_DB_PATH)
        src.row_factory = sqlite3.Row
    except Exception:
        return

    table_columns = {
        'users': ['id', 'username', 'password', 'role', 'created_at'],
        'clientes': ['id', 'nome', 'contato', 'cidade', 'created_at', 'updated_at'],
        'grupos': ['id', 'nome', 'created_at', 'updated_at'],
        'subgrupos': ['id', 'grupo_id', 'nome', 'created_at', 'updated_at'],
        'itens': ['id', 'grupo_id', 'subgrupo_id', 'nome', 'valor', 'created_at', 'updated_at'],
        'cabos': ['id', 'titulo', 'material', 'modo_corrente', 'tensao', 'corrente', 'distancia', 'secao', 'queda_max', 'metodo_instalacao', 'metodo_desc', 'isolacao', 'temperatura', 'circuitos', 'secao_max', 'paralelos_unico_conduto', 'permite_secao_menor', 'fator_potencia', 'details_json', 'created_at', 'created_by'],
        'orcamentos': ['id', 'nome', 'potencia', 'cliente_id', 'status', 'created_at', 'updated_at', 'created_by', 'edited_by', 'data_json', 'total_itens', 'capex_usina', 'preco_wp'],
    }

    try:
        for table, cols in table_columns.items():
            try:
                rows = src.execute(f"SELECT {', '.join(cols)} FROM {table}").fetchall()
            except Exception:
                continue
            if not rows:
                continue
            placeholders = ', '.join(['%s'] * len(cols))
            insert_sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            for row in rows:
                conn.execute(insert_sql, tuple(row[col] for col in cols))

        sequence_tables = ['users', 'clientes', 'grupos', 'subgrupos', 'itens', 'cabos', 'orcamentos']
        for table in sequence_tables:
            conn.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE((SELECT MAX(id) FROM {table}), 1), true)")
        conn.commit()
    finally:
        src.close()


def init_db():
    conn = get_db()
    if DATABASE_URL:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                created_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
                id SERIAL PRIMARY KEY,
                nome TEXT NOT NULL,
                contato TEXT,
                cidade TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS grupos (
                id SERIAL PRIMARY KEY,
                nome TEXT UNIQUE NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subgrupos (
                id SERIAL PRIMARY KEY,
                grupo_id INTEGER NOT NULL REFERENCES grupos(id) ON DELETE CASCADE,
                nome TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS itens (
                id SERIAL PRIMARY KEY,
                grupo_id INTEGER NOT NULL REFERENCES grupos(id) ON DELETE CASCADE,
                subgrupo_id INTEGER REFERENCES subgrupos(id) ON DELETE SET NULL,
                nome TEXT NOT NULL,
                valor DOUBLE PRECISION DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cabos (
                id SERIAL PRIMARY KEY,
                titulo TEXT,
                material TEXT,
                tensao DOUBLE PRECISION,
                corrente DOUBLE PRECISION,
                distancia DOUBLE PRECISION,
                secao DOUBLE PRECISION,
                created_at TEXT,
                created_by TEXT
            )
        """)
        ensure_column(conn, 'cabos', 'modo_corrente', "modo_corrente TEXT DEFAULT 'trifasico'")
        ensure_column(conn, 'cabos', 'queda_max', 'queda_max DOUBLE PRECISION DEFAULT 4')
        ensure_column(conn, 'cabos', 'metodo_instalacao', "metodo_instalacao TEXT DEFAULT 'B1'")
        ensure_column(conn, 'cabos', 'metodo_desc', 'metodo_desc TEXT')
        ensure_column(conn, 'cabos', 'isolacao', "isolacao TEXT DEFAULT 'PVC'")
        ensure_column(conn, 'cabos', 'temperatura', 'temperatura DOUBLE PRECISION DEFAULT 30')
        ensure_column(conn, 'cabos', 'circuitos', 'circuitos INTEGER DEFAULT 1')
        ensure_column(conn, 'cabos', 'secao_max', 'secao_max DOUBLE PRECISION DEFAULT 1000')
        ensure_column(conn, 'cabos', 'paralelos_unico_conduto', 'paralelos_unico_conduto INTEGER DEFAULT 0')
        ensure_column(conn, 'cabos', 'permite_secao_menor', 'permite_secao_menor INTEGER DEFAULT 0')
        ensure_column(conn, 'cabos', 'fator_potencia', 'fator_potencia DOUBLE PRECISION DEFAULT 0.9')
        ensure_column(conn, 'cabos', 'details_json', "details_json TEXT DEFAULT '{}' ")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orcamentos (
                id SERIAL PRIMARY KEY,
                nome TEXT NOT NULL,
                potencia DOUBLE PRECISION DEFAULT 0,
                status TEXT DEFAULT 'pendente',
                created_at TEXT,
                data_json TEXT DEFAULT '{}',
                total_itens DOUBLE PRECISION DEFAULT 0,
                capex_usina DOUBLE PRECISION DEFAULT 0,
                preco_wp DOUBLE PRECISION DEFAULT 0
            )
        """)
        ensure_column(conn, 'orcamentos', 'updated_at', 'updated_at TEXT')
        ensure_column(conn, 'orcamentos', 'created_by', 'created_by TEXT')
        ensure_column(conn, 'orcamentos', 'edited_by', 'edited_by TEXT')
        ensure_column(conn, 'orcamentos', 'cliente_id', 'cliente_id INTEGER REFERENCES clientes(id) ON DELETE SET NULL')
        conn.commit()

        users_count = conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
        grupos_count = conn.execute('SELECT COUNT(*) FROM grupos').fetchone()[0]
        orc_count = conn.execute('SELECT COUNT(*) FROM orcamentos').fetchone()[0]
        clientes_count = conn.execute('SELECT COUNT(*) FROM clientes').fetchone()[0]
        cabos_count = conn.execute('SELECT COUNT(*) FROM cabos').fetchone()[0]

        if users_count == 0 and grupos_count == 0 and orc_count == 0 and clientes_count == 0 and cabos_count == 0:
            migrate_sqlite_to_postgres(conn)

        admin = conn.execute("SELECT id FROM users WHERE username='admin'").fetchone()
        if not admin:
            conn.execute("INSERT INTO users (username, password, role, created_at) VALUES (%s, %s, %s, %s)", ('admin', 'admin', 'admin', now_str()))
        if conn.execute('SELECT COUNT(*) FROM grupos').fetchone()[0] == 0:
            for g in DEFAULT_GROUPS:
                cur = conn.execute('INSERT INTO grupos (nome, created_at, updated_at) VALUES (%s, %s, %s) RETURNING id', (g['nome'], now_str(), now_str()))
                gid = cur.fetchone()[0]
                for sg in g['subgrupos']:
                    cur2 = conn.execute('INSERT INTO subgrupos (grupo_id, nome, created_at, updated_at) VALUES (%s, %s, %s, %s) RETURNING id', (gid, sg['nome'], now_str(), now_str()))
                    sgid = cur2.fetchone()[0]
                    for item in sg['itens']:
                        conn.execute('INSERT INTO itens (grupo_id, subgrupo_id, nome, valor, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)', (gid, sgid, item['nome'], item['valor'], now_str(), now_str()))
        conn.commit()
        conn.close()
        return

    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            contato TEXT,
            cidade TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS grupos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS subgrupos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            grupo_id INTEGER NOT NULL,
            nome TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (grupo_id) REFERENCES grupos(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            grupo_id INTEGER NOT NULL,
            subgrupo_id INTEGER,
            nome TEXT NOT NULL,
            valor REAL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (grupo_id) REFERENCES grupos(id),
            FOREIGN KEY (subgrupo_id) REFERENCES subgrupos(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cabos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            titulo TEXT,
            material TEXT,
            tensao REAL,
            corrente REAL,
            distancia REAL,
            secao REAL,
            created_at TEXT,
            created_by TEXT
        )
    """)
    ensure_column(conn, 'cabos', 'modo_corrente', "modo_corrente TEXT DEFAULT 'trifasico'")
    ensure_column(conn, 'cabos', 'queda_max', 'queda_max REAL DEFAULT 4')
    ensure_column(conn, 'cabos', 'metodo_instalacao', "metodo_instalacao TEXT DEFAULT 'B1'")
    ensure_column(conn, 'cabos', 'metodo_desc', 'metodo_desc TEXT')
    ensure_column(conn, 'cabos', 'isolacao', "isolacao TEXT DEFAULT 'PVC'")
    ensure_column(conn, 'cabos', 'temperatura', 'temperatura REAL DEFAULT 30')
    ensure_column(conn, 'cabos', 'circuitos', 'circuitos INTEGER DEFAULT 1')
    ensure_column(conn, 'cabos', 'secao_max', 'secao_max REAL DEFAULT 1000')
    ensure_column(conn, 'cabos', 'paralelos_unico_conduto', 'paralelos_unico_conduto INTEGER DEFAULT 0')
    ensure_column(conn, 'cabos', 'permite_secao_menor', 'permite_secao_menor INTEGER DEFAULT 0')
    ensure_column(conn, 'cabos', 'fator_potencia', 'fator_potencia REAL DEFAULT 0.9')
    ensure_column(conn, 'cabos', 'details_json', "details_json TEXT DEFAULT '{}' ")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orcamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            potencia REAL DEFAULT 0,
            status TEXT DEFAULT 'pendente',
            created_at TEXT,
            data_json TEXT DEFAULT '{}',
            total_itens REAL DEFAULT 0,
            capex_usina REAL DEFAULT 0,
            preco_wp REAL DEFAULT 0
        )
    """)
    ensure_column(conn, 'orcamentos', 'created_at', 'created_at TEXT')
    ensure_column(conn, 'orcamentos', 'updated_at', 'updated_at TEXT')
    ensure_column(conn, 'orcamentos', 'created_by', 'created_by TEXT')
    ensure_column(conn, 'orcamentos', 'edited_by', 'edited_by TEXT')
    ensure_column(conn, 'orcamentos', 'cliente_id', 'cliente_id INTEGER')
    ensure_column(conn, 'orcamentos', 'data_json', "data_json TEXT DEFAULT '{}' ")
    ensure_column(conn, 'orcamentos', 'total_itens', 'total_itens REAL DEFAULT 0')
    ensure_column(conn, 'orcamentos', 'capex_usina', 'capex_usina REAL DEFAULT 0')
    ensure_column(conn, 'orcamentos', 'preco_wp', 'preco_wp REAL DEFAULT 0')

    orcamentos_count = conn.execute('SELECT COUNT(*) FROM orcamentos').fetchone()[0]
    if orcamentos_count == 0:
        conn.execute("DELETE FROM sqlite_sequence WHERE name='orcamentos'")

    admin = conn.execute("SELECT id FROM users WHERE username='admin'").fetchone()
    if not admin:
        conn.execute("INSERT INTO users (username, password, role, created_at) VALUES (?, ?, ?, ?)", ('admin', 'admin', 'admin', now_str()))
    if conn.execute('SELECT COUNT(*) FROM grupos').fetchone()[0] == 0:
        for g in DEFAULT_GROUPS:
            cur = conn.execute('INSERT INTO grupos (nome, created_at, updated_at) VALUES (?, ?, ?)', (g['nome'], now_str(), now_str()))
            gid = cur.lastrowid
            for sg in g['subgrupos']:
                cur2 = conn.execute('INSERT INTO subgrupos (grupo_id, nome, created_at, updated_at) VALUES (?, ?, ?, ?)', (gid, sg['nome'], now_str(), now_str()))
                sgid = cur2.lastrowid
                for item in sg['itens']:
                    conn.execute('INSERT INTO itens (grupo_id, subgrupo_id, nome, valor, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)', (gid, sgid, item['nome'], item['valor'], now_str(), now_str()))
    conn.commit()
    conn.close()


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('login'))
        return view(*args, **kwargs)
    return wrapped


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('login'))
        if session.get('role') != 'admin':
            flash('Acesso permitido apenas para administrador.')
            return redirect(url_for('dashboard'))
        return view(*args, **kwargs)
    return wrapped


@app.context_processor
def inject_current_user():
    return {
        'current_user': SimpleNamespace(
            id=session.get('user_id'),
            username=session.get('username'),
            role=session.get('role')
        )
    }


@app.template_filter('brl')
def brl(value):
    try:
        num = float(value or 0)
    except (TypeError, ValueError):
        num = 0.0
    s = f'{num:,.2f}'
    return 'R$ ' + s.replace(',', 'X').replace('.', ',').replace('X', '.')



@app.template_filter('brl4')
def brl4(value):
    try:
        num = float(value or 0)
    except (TypeError, ValueError):
        num = 0.0
    s = f'{num:,.4f}'
    return 'R$ ' + s.replace(',', 'X').replace('.', ',').replace('X', '.')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = (request.form.get('password') or '').strip()
        conn = get_db()
        user = conn.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password)).fetchone()
        conn.close()
        if user:
            session['user_id'] = user['id']
            session['username'] = user['username']
            session['role'] = user['role']
            return redirect(url_for('dashboard'))
        flash('Usuário ou senha inválidos.')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/healthz')
def healthz():
    try:
        conn = get_db()
        conn.execute('SELECT 1').fetchone()
        conn.close()
        return {'status': 'ok'}, 200
    except Exception as exc:
        return {'status': 'error', 'detail': str(exc)}, 500


@app.route('/')
@login_required
def dashboard():
    conn = get_db()
    rows = conn.execute('''
        SELECT o.*, c.nome AS cliente_nome
        FROM orcamentos o
        LEFT JOIN clientes c ON c.id = o.cliente_id
        ORDER BY o.id DESC
    ''').fetchall()
    status_counts = {'pendente': 0, 'aprovado': 0, 'recusado': 0}
    total_capex = 0.0
    for row in rows:
        status_counts[row['status'] or 'pendente'] = status_counts.get(row['status'] or 'pendente', 0) + 1
        total_capex += float(row['capex_usina'] or 0)
    conn.close()
    return render_template('dashboard.html',
                           orcamentos=rows,
                           stats=status_counts,
                           total_orcamentos=len(rows),
                           total_capex=total_capex)


@app.route('/usuarios', methods=['GET', 'POST'])
@admin_required
def usuarios():
    conn = get_db()
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = (request.form.get('password') or '').strip()
        role = request.form.get('role') or 'user'
        if username and password:
            try:
                conn.execute('INSERT INTO users (username, password, role, created_at) VALUES (?, ?, ?, ?)', (username, password, role, now_str()))
                conn.commit()
                flash('Usuário criado com sucesso.')
            except (sqlite3.IntegrityError, psycopg2.IntegrityError):
                flash('Usuário já existe.')
    rows = conn.execute('SELECT * FROM users ORDER BY id DESC').fetchall()
    conn.close()
    return render_template('usuarios.html', usuarios=rows)


@app.route('/usuarios/<int:user_id>/delete')
@admin_required
def usuario_delete(user_id):
    if user_id == session.get('user_id'):
        flash('Você não pode excluir seu próprio usuário.')
        return redirect(url_for('usuarios'))
    conn = get_db()
    conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('usuarios'))


@app.route('/clientes', methods=['GET', 'POST'])
@login_required
def clientes():
    conn = get_db()
    if request.method == 'POST':
        action = request.form.get('action', 'create')
        nome = (request.form.get('nome') or '').strip()
        contato = (request.form.get('contato') or '').strip()
        cidade = (request.form.get('cidade') or '').strip()
        if action == 'create' and nome:
            conn.execute('INSERT INTO clientes (nome, contato, cidade, created_at, updated_at) VALUES (?, ?, ?, ?, ?)', (nome, contato, cidade, now_str(), now_str()))
        elif action == 'edit':
            cid = request.form.get('cliente_id')
            conn.execute('UPDATE clientes SET nome=?, contato=?, cidade=?, updated_at=? WHERE id=?', (nome, contato, cidade, now_str(), cid))
        conn.commit()
        return redirect(url_for('clientes'))
    rows = conn.execute('SELECT * FROM clientes ORDER BY id DESC').fetchall()
    conn.close()
    return render_template('clientes.html', clientes=rows)


@app.route('/clientes/<int:cliente_id>/delete')
@login_required
def cliente_delete(cliente_id):
    conn = get_db()
    conn.execute('DELETE FROM clientes WHERE id = ?', (cliente_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('clientes'))


@app.route('/grupos', methods=['GET', 'POST'])
@login_required
def grupos():
    conn = get_db()
    if request.method == 'POST':
        kind = request.form.get('kind')
        if kind == 'grupo':
            nome = (request.form.get('nome') or '').strip()
            if nome:
                conn.execute('INSERT INTO grupos (nome, created_at, updated_at) VALUES (?, ?, ?)', (nome, now_str(), now_str()))
        elif kind == 'subgrupo':
            grupo_id = request.form.get('grupo_id')
            nome = (request.form.get('nome') or '').strip()
            if grupo_id and nome:
                conn.execute('INSERT INTO subgrupos (grupo_id, nome, created_at, updated_at) VALUES (?, ?, ?, ?)', (grupo_id, nome, now_str(), now_str()))
        elif kind == 'item':
            grupo_id = request.form.get('grupo_id')
            subgrupo_id = request.form.get('subgrupo_id') or None
            nome = (request.form.get('nome') or '').strip()
            valor = safe_float(request.form.get('valor'))
            if grupo_id and nome:
                conn.execute('INSERT INTO itens (grupo_id, subgrupo_id, nome, valor, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)', (grupo_id, subgrupo_id, nome, valor, now_str(), now_str()))
        elif kind == 'grupo_edit':
            gid = request.form.get('grupo_id')
            nome = (request.form.get('nome') or '').strip()
            conn.execute('UPDATE grupos SET nome=?, updated_at=? WHERE id=?', (nome, now_str(), gid))
        elif kind == 'subgrupo_edit':
            sgid = request.form.get('subgrupo_id')
            nome = (request.form.get('nome') or '').strip()
            gid = request.form.get('grupo_id')
            conn.execute('UPDATE subgrupos SET nome=?, grupo_id=?, updated_at=? WHERE id=?', (nome, gid, now_str(), sgid))
        elif kind == 'item_edit':
            iid = request.form.get('item_id')
            nome = (request.form.get('nome') or '').strip()
            gid = request.form.get('grupo_id')
            sgid = request.form.get('subgrupo_id') or None
            valor = safe_float(request.form.get('valor'))
            conn.execute('UPDATE itens SET nome=?, grupo_id=?, subgrupo_id=?, valor=?, updated_at=? WHERE id=?', (nome, gid, sgid, valor, now_str(), iid))
        conn.commit()
        return redirect(url_for('grupos'))
    grupos_rows = conn.execute('SELECT * FROM grupos ORDER BY nome').fetchall()
    subgrupos_rows = conn.execute('SELECT s.*, g.nome AS grupo_nome FROM subgrupos s JOIN grupos g ON g.id=s.grupo_id ORDER BY g.nome, s.nome').fetchall()
    itens_rows = conn.execute('''
        SELECT i.*, g.nome AS grupo_nome, s.nome AS subgrupo_nome
        FROM itens i JOIN grupos g ON g.id=i.grupo_id
        LEFT JOIN subgrupos s ON s.id=i.subgrupo_id
        ORDER BY g.nome, s.nome, i.nome
    ''').fetchall()
    conn.close()
    return render_template('grupos.html', grupos=grupos_rows, subgrupos=subgrupos_rows, itens=itens_rows)


@app.route('/grupos/<int:grupo_id>/delete')
@login_required
def grupo_delete(grupo_id):
    conn = get_db(); conn.execute('DELETE FROM itens WHERE grupo_id=?', (grupo_id,)); conn.execute('DELETE FROM subgrupos WHERE grupo_id=?', (grupo_id,)); conn.execute('DELETE FROM grupos WHERE id=?', (grupo_id,)); conn.commit(); conn.close(); return redirect(url_for('grupos'))


@app.route('/subgrupos/<int:subgrupo_id>/delete')
@login_required
def subgrupo_delete(subgrupo_id):
    conn = get_db(); conn.execute('DELETE FROM itens WHERE subgrupo_id=?', (subgrupo_id,)); conn.execute('DELETE FROM subgrupos WHERE id=?', (subgrupo_id,)); conn.commit(); conn.close(); return redirect(url_for('grupos'))


@app.route('/itens/<int:item_id>/delete')
@login_required
def item_delete(item_id):
    conn = get_db(); conn.execute('DELETE FROM itens WHERE id=?', (item_id,)); conn.commit(); conn.close(); return redirect(url_for('grupos'))


def load_groups_from_db():
    conn = get_db()
    grupos = conn.execute('SELECT * FROM grupos ORDER BY nome').fetchall()
    subgrupos = conn.execute('SELECT * FROM subgrupos ORDER BY nome').fetchall()
    itens = conn.execute('SELECT * FROM itens ORDER BY nome').fetchall()
    conn.close()
    group_map = []
    for g in grupos:
        sg_list = []
        for sg in [x for x in subgrupos if x['grupo_id'] == g['id']]:
            its = [{'nome': i['nome'], 'valor': i['valor']} for i in itens if i['subgrupo_id'] == sg['id']]
            sg_list.append({'nome': sg['nome'], 'itens': its})
        group_map.append({'nome': g['nome'], 'subgrupos': sg_list})
    return group_map


@app.route('/orcamento', methods=['GET', 'POST'])
@login_required
def orcamento():
    conn = get_db()
    if request.method == 'POST':
        nome = (request.form.get('nome') or '').strip() or 'Nova proposta'
        potencia = safe_float(request.form.get('potencia'))
        payload_raw = request.form.get('payload_json') or '{}'
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {}
        status = request.form.get('status') or 'pendente'
        total_itens = safe_float(request.form.get('total_itens'))
        capex_usina = safe_float(request.form.get('capex_usina'))
        preco_wp = safe_float(request.form.get('preco_wp'))
        cliente_id = request.form.get('cliente_id') or None
        conn.execute(
            """
            INSERT INTO orcamentos (nome, potencia, cliente_id, status, created_at, updated_at, created_by, edited_by, data_json, total_itens, capex_usina, preco_wp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (nome, potencia, cliente_id, status, now_str(), now_str(), session.get('username'), session.get('username'), json.dumps(payload, ensure_ascii=False), total_itens, capex_usina, preco_wp)
        )
        conn.commit(); conn.close()
        return redirect(url_for('dashboard'))
    clientes_rows = conn.execute('SELECT * FROM clientes ORDER BY nome').fetchall(); conn.close()
    return render_template('orcamento.html', groups=load_groups_from_db(), fixed_items=FIXED_ITEMS, clientes=clientes_rows, orcamento=None, ufv_type_fixed_items=UFV_TYPE_FIXED_ITEMS, ufv_types=UFV_TYPES)


@app.route('/orcamento/<int:orcamento_id>/editar', methods=['GET', 'POST'])
@login_required
def editar_orcamento(orcamento_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM orcamentos WHERE id = ?', (orcamento_id,)).fetchone()
    if not row:
        conn.close(); abort(404)
    if request.method == 'POST':
        nome = (request.form.get('nome') or '').strip() or 'Proposta'
        potencia = safe_float(request.form.get('potencia'))
        payload_raw = request.form.get('payload_json') or '{}'
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {}
        status = request.form.get('status') or (row['status'] or 'pendente')
        total_itens = safe_float(request.form.get('total_itens'))
        capex_usina = safe_float(request.form.get('capex_usina'))
        preco_wp = safe_float(request.form.get('preco_wp'))
        cliente_id = request.form.get('cliente_id') or None
        conn.execute('''
            UPDATE orcamentos SET nome=?, potencia=?, cliente_id=?, status=?, updated_at=?, edited_by=?, data_json=?, total_itens=?, capex_usina=?, preco_wp=?
            WHERE id=?
        ''', (nome, potencia, cliente_id, status, now_str(), session.get('username'), json.dumps(payload, ensure_ascii=False), total_itens, capex_usina, preco_wp, orcamento_id))
        conn.commit(); conn.close()
        return redirect(url_for('dashboard'))
    clientes_rows = conn.execute('SELECT * FROM clientes ORDER BY nome').fetchall(); conn.close()
    return render_template('orcamento.html', groups=load_groups_from_db(), fixed_items=FIXED_ITEMS, clientes=clientes_rows, orcamento=row, ufv_type_fixed_items=UFV_TYPE_FIXED_ITEMS, ufv_types=UFV_TYPES)


@app.route('/orcamento/<int:orcamento_id>')
@login_required
def visualizar_orcamento(orcamento_id):
    conn = get_db()
    row = conn.execute('SELECT o.*, c.nome as cliente_nome FROM orcamentos o LEFT JOIN clientes c ON c.id=o.cliente_id WHERE o.id = ?', (orcamento_id,)).fetchone()
    conn.close()
    if not row:
        abort(404)
    try:
        payload = json.loads(row['data_json'] or '{}')
    except json.JSONDecodeError:
        payload = {}

    potencia_wp = float(row['potencia'] or 0) * 1000
    material_groups = []
    grouped = {}
    for item in payload.get('materiais', []) or []:
        total = safe_float(item.get('total')) or (safe_float(item.get('qtd'), 0) * safe_float(item.get('valor'), 0))
        grupo = (item.get('grupo') or 'Sem grupo').strip() or 'Sem grupo'
        normalized = {
            'grupo': grupo,
            'subgrupo': item.get('subgrupo') or '-',
            'item': item.get('item') or '-',
            'qtd': safe_float(item.get('qtd'), 0),
            'valor': safe_float(item.get('valor'), 0),
            'total': total,
            'rwp': (total / potencia_wp) if potencia_wp > 0 else 0,
        }
        grouped.setdefault(grupo, []).append(normalized)

    for grupo_nome, rows in grouped.items():
        material_groups.append({
            'grupo': grupo_nome,
            'rows': rows,
            'total': sum(r['total'] for r in rows),
            'rwp': (sum(r['total'] for r in rows) / potencia_wp) if potencia_wp > 0 else 0,
        })
    material_groups.sort(key=lambda g: g['grupo'].lower())

    fixed_rows = []
    for item in payload.get('fixos', []) or []:
        valor = safe_float(item.get('valor'), 0)
        fixed_rows.append({
            'nome': item.get('nome') or '-',
            'valor': valor,
            'rwp': (valor / potencia_wp) if potencia_wp > 0 else 0,
        })

    kit_rows = []
    for item in payload.get('kit_fotovoltaico', []) or []:
        valor = safe_float(item.get('valor'), 0)
        kit_rows.append({
            'nome': item.get('nome') or '-',
            'valor': valor,
            'rwp': (valor / potencia_wp) if potencia_wp > 0 else 0,
        })

    return render_template('orcamento_view.html', orcamento=row, payload=payload, potencia_wp=potencia_wp, material_groups=material_groups, fixed_rows=fixed_rows, kit_rows=kit_rows)


@app.route('/orcamento/<int:orcamento_id>/status/<status>')
@login_required
def alterar_status(orcamento_id, status):
    if status not in {'pendente', 'aprovado', 'recusado'}:
        abort(400)
    conn = get_db(); conn.execute('UPDATE orcamentos SET status = ?, updated_at=?, edited_by=? WHERE id = ?', (status, now_str(), session.get('username'), orcamento_id)); conn.commit(); conn.close(); return redirect(url_for('dashboard'))


@app.route('/orcamento/<int:orcamento_id>/exportar-pdf')
@login_required
def exportar_orcamento_pdf(orcamento_id):
    export_ctx = build_export_context(orcamento_id)
    if not export_ctx:
        abort(404)
    pdf_buffer = generate_orcamento_pdf(export_ctx)
    filename = safe_filename(export_ctx['orcamento']['nome']) + '.pdf'
    return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)


@app.route('/orcamento/<int:orcamento_id>/delete')
@login_required
def delete_orcamento(orcamento_id):
    conn = get_db(); conn.execute('DELETE FROM orcamentos WHERE id = ?', (orcamento_id,)); conn.commit(); conn.close(); return redirect(url_for('dashboard'))


@app.route('/cabos', methods=['GET', 'POST'])
@login_required
def cabos():
    conn = get_db()
    if request.method == 'POST':
        titulo = (request.form.get('titulo') or '').strip()
        material = request.form.get('material') or 'cobre'
        modo_corrente = request.form.get('modo_corrente') or 'trifasico'
        tensao = safe_float(request.form.get('tensao'))
        corrente = safe_float(request.form.get('corrente'))
        distancia = safe_float(request.form.get('distancia'))
        secao = safe_float(request.form.get('secao'))
        queda_max = safe_float(request.form.get('queda_max'), 4.0)
        metodo_instalacao = request.form.get('metodo_instalacao') or 'B1'
        metodo_desc = request.form.get('metodo_desc') or ''
        isolacao = request.form.get('isolacao') or 'PVC'
        temperatura = safe_float(request.form.get('temperatura'), 30.0)
        circuitos = int(safe_float(request.form.get('circuitos'), 1))
        secao_max = safe_float(request.form.get('secao_max'), 1000.0)
        paralelos_unico_conduto = 1 if request.form.get('paralelos_unico_conduto') else 0
        permite_secao_menor = 1 if request.form.get('permite_secao_menor') else 0
        fator_potencia = safe_float(request.form.get('fator_potencia'), 0.9)
        details = {
            'carga': safe_float(request.form.get('carga')),
            'unidade_carga': request.form.get('unidade_carga') or 'W',
            'observacoes': request.form.get('observacoes') or '',
        }
        conn.execute(
            'INSERT INTO cabos (titulo, material, modo_corrente, tensao, corrente, distancia, secao, queda_max, metodo_instalacao, metodo_desc, isolacao, temperatura, circuitos, secao_max, paralelos_unico_conduto, permite_secao_menor, fator_potencia, details_json, created_at, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (titulo, material, modo_corrente, tensao, corrente, distancia, secao, queda_max, metodo_instalacao, metodo_desc, isolacao, temperatura, circuitos, secao_max, paralelos_unico_conduto, permite_secao_menor, fator_potencia, json.dumps(details, ensure_ascii=False), now_str(), session.get('username'))
        )
        conn.commit()
    rows = conn.execute('SELECT * FROM cabos ORDER BY id DESC').fetchall()
    conn.close()
    return render_template('cabos.html', cabos=rows)


@app.route('/cabos/<int:cabo_id>/delete')
@login_required
def cabo_delete(cabo_id):
    conn = get_db(); conn.execute('DELETE FROM cabos WHERE id=?', (cabo_id,)); conn.commit(); conn.close(); return redirect(url_for('cabos'))


init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(host='0.0.0.0', port=port, debug=debug)
