import os
import re
import sqlite3
import unicodedata
import tempfile
import requests
import webbrowser
import threading
from datetime import datetime
import concurrent.futures
from bs4 import BeautifulSoup
import fitz
from pdf2image import convert_from_path
import pytesseract
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import time
from threading import Thread
import sys
import logging

# ========== SETUP DE LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuração de caminhos baseada no ambiente (PyInstaller ou Script normal)
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CAMINHO_POPPLER = os.path.join(BASE_DIR, 'poppler', 'Library', 'bin')
BASE_URL_IOEPA = "https://www.ioepa.com.br/arquivos/"
DB_FILE = os.path.join(BASE_DIR, "indice_doe.db")

# Configuração proativa do Tesseract
CAMINHO_TESSERACT = os.path.join(BASE_DIR, 'tesseract', 'tesseract.exe')
if os.path.exists(CAMINHO_TESSERACT):
    pytesseract.pytesseract.tesseract_cmd = CAMINHO_TESSERACT

# ========== FUNÇÕES DE VALIDAÇÃO E NORMALIZAÇÃO ==========

def normalizar_texto(texto):
    if not texto: return ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto = texto.lower()
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()

def validar_data(ano, mes, dia):
    """Valida se ano/mes/dia formam uma data real."""
    try:
        if not all(isinstance(x, int) for x in [ano, mes, dia]):
            return False
        if ano < 1900 or ano > 2100:
            return False
        if mes < 1 or mes > 12:
            return False
        if dia < 1 or dia > 31:
            return False
        
        datetime(ano, mes, dia) # O datetime levanta ValueError se não existir (ex: 31/02)
        return True
    except (ValueError, TypeError) as e:
        logger.warning(f"Data inválida: {ano}-{mes}-{dia} | Erro: {e}")
        return False

def extrair_data_nome(nome_arquivo):
    """Extrai data usando Regex com validação robusta para evitar anomalias."""
    try:
        if not nome_arquivo or not isinstance(nome_arquivo, str):
            return None, None, None, None

        # Tenta padrão Internacional ou ISO: YYYY.MM.DD, YYYY-MM-DD, YYYY_MM_DD
        match = re.search(r'(\d{4})[._-](\d{2})[._-](\d{2})', nome_arquivo)
        if match:
            ano, mes, dia = map(int, match.groups())
        else:
            # Fallback: Tenta padrão Brasileiro: DD.MM.YYYY, DD-MM-YYYY, DD_MM_YYYY
            match_br = re.search(r'(\d{2})[._-](\d{2})[._-](\d{4})', nome_arquivo)
            if match_br:
                dia, mes, ano = map(int, match_br.groups())
            else:
                return None, None, None, None

        if not validar_data(ano, mes, dia):
            logger.warning(f"Data inválida extraída de {nome_arquivo}: {ano}-{mes}-{dia}")
            return None, None, None, None

        data_str = f"{dia:02d}/{mes:02d}/{ano}"
        return data_str, ano, mes, dia
    except Exception as e:
        logger.error(f"Erro inesperado em extrair_data_nome({nome_arquivo}): {e}")
        return None, None, None, None

def validar_entrada_busca(nome, cpf=None, data_ini=None, data_fim=None):
    """Valida entrada do usuário para a interface de busca com ranges de data."""
    if not nome or not isinstance(nome, str):
        return False, "O Nome é obrigatório."
    
    nome_limpo = nome.strip()
    if len(nome_limpo) < 3:
        return False, "O Nome deve ter pelo menos 3 caracteres."
    
    if not any(c.isalpha() for c in nome_limpo):
        return False, "O Nome deve conter pelo menos uma letra."
    
    if cpf and cpf.strip():
        cpf_nums = re.sub(r'\D', '', cpf)
        if len(cpf_nums) != 11:
            return False, "O CPF deve ter 11 dígitos."
            
    def verificar_data_periodo(texto_data, label):
        if not texto_data: return None, "" # Vazio é permitido
        
        # Separa a string "DD/MM/AAAA"
        partes = texto_data.split('/')
        if len(partes) != 3 or len(texto_data) != 10: 
            return None, f"A data '{label}' deve estar no formato completo DD/MM/AAAA."
        
        try:
            d_int, m_int, y_int = int(partes[0]), int(partes[1]), int(partes[2])
            if not validar_data(y_int, m_int, d_int): return None, f"A data '{label}' é inválida ou não existe no calendário."
            if y_int < 1980 or y_int > datetime.now().year: return None, f"O Ano '{label}' deve estar entre 1980 e {datetime.now().year}."
            return datetime(y_int, m_int, d_int), ""
        except ValueError:
            return None, f"A data '{label}' deve conter apenas números e barras."

    dt_inicio, erro_ini = verificar_data_periodo(data_ini, "De")
    if erro_ini: return False, erro_ini
    
    dt_final, erro_fim = verificar_data_periodo(data_fim, "Até")
    if erro_fim: return False, erro_fim
    
    if dt_inicio and dt_final and dt_inicio > dt_final:
        return False, "A data 'De' (Início) não pode ser maior que a data 'Até' (Fim)."
    
    return True, ""

# ========== CLASSES PRINCIPAIS ==========

class IndiceSQLite:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self.db_lock = threading.Lock()
        self._inicializar_tabelas()

    def conectar(self):
        return sqlite3.connect(self.db_path, timeout=60.0)

    def _inicializar_tabelas(self):
        with self.db_lock:
            conn = None
            try:
                conn = self.conectar()
                conn.execute("PRAGMA journal_mode=WAL;")
                cursor = conn.cursor()
                
                cursor.execute("PRAGMA table_info(controle_downloads)")
                colunas = cursor.fetchall()
                
                precisa_migrar = False
                if colunas and colunas[0][1] == "data_publicacao":
                    precisa_migrar = True
                
                if precisa_migrar:
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS controle_downloads_novo (
                            arquivo TEXT PRIMARY KEY,
                            data_publicacao DATE
                        )
                    ''')
                    cursor.execute('''
                        INSERT OR IGNORE INTO controle_downloads_novo 
                        SELECT arquivo, data_publicacao FROM controle_downloads
                    ''')
                    cursor.execute('DROP TABLE controle_downloads')
                    cursor.execute('ALTER TABLE controle_downloads_novo RENAME TO controle_downloads')
                else:
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS controle_downloads (
                            arquivo TEXT PRIMARY KEY,
                            data_publicacao DATE
                        )
                    ''')
                
                cursor.execute('''
                    CREATE VIRTUAL TABLE IF NOT EXISTS diarios USING fts5(
                        arquivo, data, ano UNINDEXED, mes UNINDEXED, dia UNINDEXED, pagina UNINDEXED, texto,
                        tokenize='unicode61 remove_diacritics 1'
                    )
                ''')
                conn.commit()
            except Exception as e:
                logger.error(f"Erro ao inicializar tabelas: {e}")
                if conn: conn.rollback()
            finally:
                if conn:
                    try: conn.close()
                    except Exception as e: logger.warning(f"Erro ao fechar conexão no Init: {e}")

    def obter_datas_baixadas(self):
        conn = None
        try:
            conn = self.conectar()
            cursor = conn.cursor()
            cursor.execute("SELECT arquivo FROM controle_downloads")
            return set(row[0] for row in cursor.fetchall())
        except Exception as e:
            logger.error(f"Erro ao listar baixados: {e}")
            return set()
        finally:
            if conn: conn.close()

    def salvar_paginas_db(self, paginas_extraidas, arquivo, data_str):
        if not paginas_extraidas:
            logger.info(f"Nenhuma página a salvar para o arquivo {arquivo}.")
            return True
        
        try:
            datetime.strptime(data_str, "%d/%m/%Y")
        except ValueError:
            logger.error(f"Data inválida barrada antes de salvar: {data_str}")
            return False

        with self.db_lock:
            for tentativa in range(3):
                conn = None
                try:
                    conn = self.conectar()
                    cursor = conn.cursor()
                    
                    for pag in paginas_extraidas:
                        cursor.execute('''
                            INSERT INTO diarios (arquivo, data, ano, mes, dia, pagina, texto)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (pag['arquivo'], pag['data'], pag['ano'], pag['mes'], pag['dia'], pag['pagina'], pag['texto']))
                    
                    data_db = datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
                    cursor.execute("INSERT OR IGNORE INTO controle_downloads (arquivo, data_publicacao) VALUES (?, ?)", (arquivo, data_db))
                    
                    conn.commit()
                    logger.info(f"Salvo {len(paginas_extraidas)} páginas do {arquivo}")
                    return True
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower():
                        logger.warning(f"DB Locked na tentativa {tentativa+1}. Aguardando...")
                        time.sleep(2 ** tentativa)
                    else:
                        logger.error(f"Erro DB salvar {arquivo}: {e}")
                        return False
                except Exception as e:
                    logger.error(f"Erro geral DB em {arquivo}: {e}")
                    return False
                finally:
                    if conn:
                        try: conn.close()
                        except: pass
        
        logger.error(f"Falha ao salvar {arquivo} após 3 tentativas (Lock)")
        return False

    def buscar(self, nome, cpf=None, data_ini=None, data_fim=None):
        nome_norm = normalizar_texto(nome)
        cpf_norm = re.sub(r'\D', '', cpf) if cpf else None
        
        if not nome_norm: return []

        query_match = f'"{nome_norm}"'
        if cpf_norm: query_match += f' AND "{cpf_norm}"'

        sql = "SELECT arquivo, data, ano, pagina FROM diarios WHERE diarios MATCH ?"
        params = [query_match]

        try:
            if data_ini:
                d, m, y = map(int, data_ini.split('/'))
                val_ini = y * 10000 + m * 100 + d
                sql += " AND (CAST(ano AS INTEGER) * 10000 + CAST(mes AS INTEGER) * 100 + CAST(dia AS INTEGER)) >= ?"
                params.append(val_ini)
                
            if data_fim:
                d, m, y = map(int, data_fim.split('/'))
                val_fim = y * 10000 + m * 100 + d
                sql += " AND (CAST(ano AS INTEGER) * 10000 + CAST(mes AS INTEGER) * 100 + CAST(dia AS INTEGER)) <= ?"
                params.append(val_fim)
        except ValueError:
            logger.error("Falha silenciosa prevenida: Conversão matemática de data falhou.")
            return []

        sql += " LIMIT 300"

        conn = None
        linhas = []
        try:
            conn = self.conectar()
            cursor = conn.cursor()
            cursor.execute(sql, params)
            linhas = cursor.fetchall()
        except Exception as e:
            logger.error(f"Erro executando query: {e}")
        finally:
            if conn: conn.close()

        resultados = [{'arquivo': l[0], 'data': l[1], 'ano': l[2], 'pagina': l[3]} for l in linhas]
        try:
            resultados.sort(key=lambda x: datetime.strptime(x['data'], '%d/%m/%Y'), reverse=True)
        except Exception as e:
            logger.warning(f"Erro ordenando datas: {e}")
        return resultados

class AtualizadorDOE:
    def __init__(self, db, callback_status=None, callback_progresso=None):
        self.db = db
        self.callback_status = callback_status
        self.callback_progresso = callback_progresso
        self.MAX_PDF_SIZE = 100 * 1024 * 1024  # Proteção OOM: 100 MB max

    def buscar_links_disponiveis(self):
        arquivos_ja_baixados = self.db.obter_datas_baixadas()
        links_para_baixar = []
        
        ano_atual = datetime.now().year
        anos_verificacao = [ano_atual, ano_atual - 1]
        
        for ano in anos_verificacao:
            try:
                url_ano = f"{BASE_URL_IOEPA}{ano}/"
                resposta = requests.get(url_ano, timeout=30)
                if resposta.status_code != 200: continue
                    
                soup = BeautifulSoup(resposta.text, 'html.parser')
                for a_tag in soup.find_all('a'):
                    href = a_tag.get('href')
                    if href and href.lower().endswith('.pdf'):
                        nome_arquivo = href.split('/')[-1]
                        if nome_arquivo not in arquivos_ja_baixados:
                            url_completa = href if href.startswith('http') else f"{url_ano}{nome_arquivo}"
                            links_para_baixar.append({'url': url_completa, 'arquivo': nome_arquivo})
            except Exception as e:
                logger.error(f"Erro repo ano {ano}: {e}")
            
        return links_para_baixar

    def processar_pdf_download(self, item):
        url = item['url']
        arquivo = item['arquivo']
        data_str, ano, mes, dia = extrair_data_nome(arquivo)
        
        if not data_str: return False

        paginas_extraidas = []
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
            temp_pdf_path = temp_pdf.name
            try:
                resposta = requests.get(url, stream=True, timeout=60)
                resposta.raise_for_status()
                
                tamanho_atual = 0
                for chunk in resposta.iter_content(chunk_size=8192):
                    tamanho_atual += len(chunk)
                    if tamanho_atual > self.MAX_PDF_SIZE:
                        logger.error(f"Arquivo {arquivo} excede 100MB. Ignorado.")
                        return False
                    temp_pdf.write(chunk)
                temp_pdf.flush()
                
                try:
                    if ano >= 2008:
                        doc = fitz.open(temp_pdf_path)
                        for num_pagina, pagina in enumerate(doc, start=1):
                            texto = normalizar_texto(pagina.get_text())
                            if texto:
                                paginas_extraidas.append({
                                    'arquivo': arquivo, 'data': data_str, 'ano': ano, 
                                    'mes': mes, 'dia': dia, 'pagina': num_pagina, 'texto': texto
                                })
                        doc.close()
                    else:
                        paginas = convert_from_path(temp_pdf_path, dpi=150, poppler_path=CAMINHO_POPPLER, grayscale=True)
                        for num_pagina, img in enumerate(paginas, start=1):
                            texto = normalizar_texto(pytesseract.image_to_string(img, lang='por'))
                            if texto:
                                paginas_extraidas.append({
                                    'arquivo': arquivo, 'data': data_str, 'ano': ano, 
                                    'mes': mes, 'dia': dia, 'pagina': num_pagina, 'texto': texto
                                })
                                
                    if not paginas_extraidas:
                        logger.warning(f"Arquivo {arquivo} processado mas sem texto (talvez imagem corrompida).")
                        return False
                        
                    return self.db.salvar_paginas_db(paginas_extraidas, arquivo, data_str)
                    
                except Exception as e:
                    logger.error(f"Erro corrupção/conversão PDF {arquivo}: {e}")
                    return False
            except requests.RequestException as e:
                logger.error(f"Erro de rede baixando {arquivo}: {e}")
                return False
            finally:
                temp_pdf.close()
                if os.path.exists(temp_pdf_path):
                    try: os.remove(temp_pdf_path)
                    except: pass

    def iniciar_atualizacao(self):
        if self.callback_status: self.callback_status("Verificando IOEPA...", "#F59E0B", True)
        links = self.buscar_links_disponiveis()
        
        total = len(links)
        if total == 0:
            if self.callback_status: self.callback_status("Base atualizada.", "#10B981", False)
            return

        if self.callback_status: self.callback_status(f"Processando {total} diários...", "#F59E0B", True, 'determinate')
        
        concluidos = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futuros = {executor.submit(self.processar_pdf_download, link): link for link in links}
            for futuro in concurrent.futures.as_completed(futuros):
                concluidos += 1
                if self.callback_progresso: self.callback_progresso(concluidos, total)

        try:
            with self.db.db_lock:
                conn = self.db.conectar()
                conn.execute("INSERT INTO diarios(diarios) VALUES('optimize')")
                conn.commit()
                conn.close()
        except Exception as e:
            logger.error(f"Erro Otimização Final DB: {e}")

        if self.callback_status: self.callback_status("Atualização concluída.", "#10B981", False)

class EstiloUI:
    CORES = {
        'principal': '#0055A4', 'fundo_app': '#F0F2F5', 'fundo_card': '#FFFFFF', 
        'texto_primario': '#111827', 'texto_secundario': '#4B5563', 'borda': '#D1D5DB', 
        'sucesso': '#10B981', 'erro': '#EF4444', 'alerta': '#F59E0B', 'botao_secundario': '#6B7280'
    }
    FONTES = {
        'label': ('Segoe UI', 9, 'bold'), 'input': ('Segoe UI', 10), 'botao': ('Segoe UI', 9, 'bold'),
        'resultado_header': ('Segoe UI', 10, 'bold'), 'resultado_texto': ('Consolas', 10), 'status': ('Segoe UI', 9)
    }

# ========== COMPONENTE CUSTOMIZADO (PLACEHOLDER) ==========
class EntryComPlaceholder(tk.Entry):
    def __init__(self, master=None, placeholder="PLACEHOLDER", color_placeholder=EstiloUI.CORES['texto_secundario'], color_text=EstiloUI.CORES['texto_primario'], **kwargs):
        super().__init__(master, **kwargs)
        self.placeholder = placeholder
        self.color_placeholder = color_placeholder
        self.color_text = color_text
        
        self.bind("<FocusIn>", self._focus_in)
        self.bind("<FocusOut>", self._focus_out)
        self._colocar_placeholder()

    def _colocar_placeholder(self):
        self.insert(0, self.placeholder)
        self.config(fg=self.color_placeholder)

    def _focus_in(self, event):
        if super().get() == self.placeholder:
            self.delete(0, tk.END)
            self.config(fg=self.color_text)

    def _focus_out(self, event):
        if not super().get():
            self._colocar_placeholder()

    def get(self):
        """Retorna vazio se o texto atual for o placeholder."""
        valor = super().get()
        if valor == self.placeholder:
            return ""
        return valor
        
    def limpar_tudo(self):
        """Limpa e recoloca o placeholder ativamente."""
        self.delete(0, tk.END)
        self._colocar_placeholder()


class EntryDataMask(EntryComPlaceholder):
    """Entry com placeholder que adiciona a máscara DD/MM/AAAA automaticamente."""
    def __init__(self, master=None, placeholder="DD/MM/AAAA", **kwargs):
        super().__init__(master, placeholder=placeholder, **kwargs)
        self.bind('<KeyRelease>', self._aplicar_mascara)

    def _aplicar_mascara(self, event):
        # Ignora teclas de navegação e exclusão para não bugar a experiência do usuário
        if event.keysym in ('BackSpace', 'Delete', 'Left', 'Right', 'Up', 'Down', 'Tab'):
            return
        
        # Pega o valor usando o super de tk.Entry para não trigar a lógica de vazio do placeholder
        texto = tk.Entry.get(self)
        if texto == self.placeholder: return
        
        # Remove tudo que não for número
        numeros = re.sub(r'\D', '', texto)
        
        # Formata com as barras
        formatado = ""
        for i in range(len(numeros)):
            if i in (2, 4): formatado += '/'
            formatado += numeros[i]
        
        # Limita a 10 caracteres
        formatado = formatado[:10]
        
        # Atualiza a view
        if texto != formatado:
            self.delete(0, tk.END)
            self.insert(0, formatado)

class BDOEApp:
    def __init__(self, root):
        self.root = root
        self.root.title("BDOE - Polícia Civil")
        self.root.geometry("850x650")
        self.root.minsize(800, 600)
        self.root.configure(bg=EstiloUI.CORES['fundo_app'])

        try:
            caminho_icone = os.path.join(sys._MEIPASS, "icone.ico")
        except:
            caminho_icone = os.path.join(BASE_DIR, "icone.ico")
            
        if os.path.exists(caminho_icone):
            self.root.iconbitmap(caminho_icone)
        
        self.db = IndiceSQLite()
        self.links_armazenados = {}
        
        self._configurar_estilos()
        self.construir_interface()
        self.atualizar_status("BDOE Operacional", EstiloUI.CORES['sucesso'])

    def _configurar_estilos(self):
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("TProgressbar", thickness=15, background=EstiloUI.CORES['sucesso'], troughcolor=EstiloUI.CORES['borda'])

    def construir_interface(self):
        frame_header = tk.Frame(self.root, bg=EstiloUI.CORES['principal'], height=70)
        frame_header.pack(fill=tk.X, side=tk.TOP)
        frame_header.pack_propagate(False)
        container_header = tk.Frame(frame_header, bg=EstiloUI.CORES['principal'])
        container_header.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        tk.Label(container_header, text="BDOE - Buscador de Diário Oficial do Estado", font=('Segoe UI', 18, 'bold'), bg=EstiloUI.CORES['principal'], fg="#FFFFFF").pack(side=tk.LEFT)
        tk.Label(container_header, text="POLÍCIA CIVIL DO PARÁ", font=('Segoe UI', 9, 'bold'), bg=EstiloUI.CORES['principal'], fg="#93C5FD").pack(side=tk.RIGHT, pady=(5,0))

        frame_status = tk.Frame(self.root, bg="#E5E7EB", height=35)
        frame_status.pack(fill=tk.X, side=tk.BOTTOM)
        frame_status.pack_propagate(False)
        self.lbl_status = tk.Label(frame_status, text="Iniciando...", font=EstiloUI.FONTES['status'], bg="#E5E7EB", fg=EstiloUI.CORES['texto_secundario'])
        self.lbl_status.pack(side=tk.LEFT, padx=15, pady=5)
        self.progress_bar = ttk.Progressbar(frame_status, mode='determinate', length=150)

        main_container = tk.Frame(self.root, bg=EstiloUI.CORES['fundo_app'])
        main_container.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        card_busca = tk.Frame(main_container, bg=EstiloUI.CORES['fundo_card'], bd=1, relief=tk.SOLID)
        card_busca.configure(highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1, bd=0)
        card_busca.pack(fill=tk.X, pady=(0, 15))
        
        inner_busca = tk.Frame(card_busca, bg=EstiloUI.CORES['fundo_card'])
        inner_busca.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        tk.Label(inner_busca, text="Nome Completo da pessoa *", font=EstiloUI.FONTES['label'], bg=EstiloUI.CORES['fundo_card']).grid(row=0, column=0, sticky=tk.W)
        self.entry_nome = tk.Entry(inner_busca, font=EstiloUI.FONTES['input'], width=45, relief=tk.FLAT, highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1)
        self.entry_nome.grid(row=1, column=0, sticky=tk.W, pady=(2, 10), ipady=3)
        
        tk.Label(inner_busca, text="CPF (Opcional)", font=EstiloUI.FONTES['label'], bg=EstiloUI.CORES['fundo_card']).grid(row=0, column=1, sticky=tk.W, padx=(15, 0))
        self.entry_cpf = tk.Entry(inner_busca, font=EstiloUI.FONTES['input'], width=20, relief=tk.FLAT, highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1)
        self.entry_cpf.grid(row=1, column=1, sticky=tk.W, padx=(15, 0), pady=(2, 10), ipady=3)

        # Atualização: Label e Frame para o filtro de Período
        tk.Label(inner_busca, text="Filtro por Período (Opcional):", font=EstiloUI.FONTES['label'], bg=EstiloUI.CORES['fundo_card']).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=(5, 2))
        frame_datas = tk.Frame(inner_busca, bg=EstiloUI.CORES['fundo_card'])
        frame_datas.grid(row=3, column=0, columnspan=2, sticky=tk.W)
        
        # Grupo "De:"
        tk.Label(frame_datas, text="De:", font=EstiloUI.FONTES['status'], bg=EstiloUI.CORES['fundo_card']).pack(side=tk.LEFT, padx=(0, 5))
        self.entry_data_ini = EntryDataMask(frame_datas, width=12, justify='center', font=EstiloUI.FONTES['input'], relief=tk.FLAT, highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1)
        self.entry_data_ini.pack(side=tk.LEFT, padx=(0, 20), ipady=3)

        # Grupo "Até:"
        tk.Label(frame_datas, text="Até:", font=EstiloUI.FONTES['status'], bg=EstiloUI.CORES['fundo_card']).pack(side=tk.LEFT, padx=(0, 5))
        self.entry_data_fim = EntryDataMask(frame_datas, width=12, justify='center', font=EstiloUI.FONTES['input'], relief=tk.FLAT, highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1)
        self.entry_data_fim.pack(side=tk.LEFT, padx=(0, 10), ipady=3)   

        frame_botoes = tk.Frame(inner_busca, bg=EstiloUI.CORES['fundo_card'])
        frame_botoes.grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=(15, 0))

        self.btn_buscar = tk.Button(frame_botoes, text="🔍 BUSCAR", font=EstiloUI.FONTES['botao'], bg=EstiloUI.CORES['principal'], fg="#FFFFFF", relief=tk.FLAT, padx=15, pady=6, command=self.iniciar_busca)
        self.btn_buscar.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_limpar = tk.Button(frame_botoes, text="LIMPAR", font=EstiloUI.FONTES['botao'], bg=EstiloUI.CORES['botao_secundario'], fg="#FFFFFF", relief=tk.FLAT, padx=15, pady=6, command=self.limpar_campos)
        self.btn_limpar.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_atualizar = tk.Button(frame_botoes, text="⬇️ ATUALIZAR", font=EstiloUI.FONTES['botao'], bg=EstiloUI.CORES['alerta'], fg="#FFFFFF", relief=tk.FLAT, padx=15, pady=6, command=self.acionar_atualizacao)
        self.btn_atualizar.pack(side=tk.LEFT)


        botoes_entrada = [
            self.entry_nome, 
            self.entry_cpf, 
            self.entry_data_ini, 
            self.entry_data_fim
        ]
        for entry in botoes_entrada: 
            entry.bind('<Return>', lambda e: self.iniciar_busca())

        card_resultados = tk.Frame(main_container, bg=EstiloUI.CORES['fundo_card'], bd=1, relief=tk.SOLID)
        card_resultados.configure(highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1, bd=0)
        card_resultados.pack(fill=tk.BOTH, expand=True)
        
        header_resultado = tk.Frame(card_resultados, bg="#F9FAFB", bd=1, relief=tk.SOLID)
        header_resultado.config(highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1, bd=0)
        header_resultado.pack(fill=tk.X)
        self.lbl_contador = tk.Label(header_resultado, text="Resultados (0)", font=EstiloUI.FONTES['resultado_header'], bg="#F9FAFB", pady=8, padx=15)
        self.lbl_contador.pack(anchor=tk.W)

        self.text_resultado = scrolledtext.ScrolledText(card_resultados, font=EstiloUI.FONTES['resultado_texto'], bg=EstiloUI.CORES['fundo_card'], state=tk.DISABLED, wrap=tk.WORD, relief=tk.FLAT, bd=0, padx=15, pady=10)
        self.text_resultado.pack(fill=tk.BOTH, expand=True)
        
        self.text_resultado.tag_config("link", foreground="#2563EB", underline=True)
        self.text_resultado.tag_bind("link", "<Enter>", lambda e: self.text_resultado.config(cursor="hand2"))
        self.text_resultado.tag_bind("link", "<Leave>", lambda e: self.text_resultado.config(cursor=""))
        self.text_resultado.tag_bind("link", "<Button-1>", self.abrir_link)

    def atualizar_status(self, mensagem, cor, show_progress=False, progress_mode='indeterminate'):
        def _update():
            self.lbl_status.config(text=mensagem, fg=cor)
            if show_progress:
                self.progress_bar.config(mode=progress_mode)
                self.progress_bar.pack(side=tk.RIGHT, padx=15, pady=5)
                if progress_mode == 'indeterminate': self.progress_bar.start(10)
            else:
                self.progress_bar.stop()
                self.progress_bar.pack_forget()
        self.root.after(0, _update)

    def progresso_atualizacao(self, atual, total):
        pct = (atual / total) * 100 if total > 0 else 0
        def _update():
            self.lbl_status.config(text=f"Processando: {pct:.1f}% ({atual}/{total})", fg=EstiloUI.CORES['alerta'])
            self.progress_bar.config(mode='determinate', maximum=total, value=atual)
        self.root.after(0, _update)

    def acionar_atualizacao(self):
        self.btn_atualizar.config(state=tk.DISABLED)
        self.btn_buscar.config(state=tk.DISABLED)
        atualizador = AtualizadorDOE(self.db, self.atualizar_status, self.progresso_atualizacao)
        
        def _thread():
            try:
                atualizador.iniciar_atualizacao()
            except Exception as e:
                logger.error(f"Thread atualização falhou fatalmente: {e}")
                self.atualizar_status("Erro fatal na atualização.", EstiloUI.CORES['erro'])
            finally:
                self.root.after(0, lambda: self.btn_atualizar.config(state=tk.NORMAL))
                self.root.after(0, lambda: self.btn_buscar.config(state=tk.NORMAL))
                
        Thread(target=_thread, daemon=True).start()

    def limpar_campos(self):
        # Limpa os campos padrão
        self.entry_nome.delete(0, tk.END)
        self.entry_cpf.delete(0, tk.END)
        
        # Usa a função limpar_tudo() da nossa classe customizada 
        # para restaurar os placeholders
        self.entry_data_ini.limpar_tudo()
        self.entry_data_fim.limpar_tudo()
            
        # Limpa os resultados e o contador
        self.text_resultado.config(state=tk.NORMAL)
        self.text_resultado.delete(1.0, tk.END)
        self.text_resultado.config(state=tk.DISABLED)
        self.lbl_contador.config(text="Resultados (0)")

    def iniciar_busca(self):
        nome = self.entry_nome.get().strip()
        cpf = self.entry_cpf.get().strip()
        
        data_ini = self.entry_data_ini.get().strip()
        data_fim = self.entry_data_fim.get().strip()
        
        # Validando as entradas primeiro
        valido, erro_msg = validar_entrada_busca(nome, cpf, data_ini, data_fim)
        if not valido:
            messagebox.showwarning("Atenção - Busca Inválida", erro_msg)
            return

        self.btn_buscar.config(state=tk.DISABLED)
        self.atualizar_status("Buscando...", EstiloUI.CORES['alerta'], True, 'indeterminate')
        self.text_resultado.config(state=tk.NORMAL)
        self.text_resultado.delete(1.0, tk.END)
        self.links_armazenados.clear()
        
        Thread(target=self._executar, args=(nome, cpf, data_ini, data_fim), daemon=True).start()

    def _executar(self, nome, cpf, data_ini, data_fim):
        res = self.db.buscar(nome, cpf, data_ini, data_fim)

        def _concluir():
            if not res:
                self.text_resultado.insert(tk.END, "\n   Nenhum registro encontrado com estes parâmetros.")
                aviso_limite = ""
            else:
                for i, r in enumerate(res, 1):
                    self.text_resultado.insert(tk.END, f"   OCORRÊNCIA #{i:03d}\n")
                    self.text_resultado.insert(tk.END, f"   Data: {r['data']} | Página: {r['pagina']}\n   ")
                    
                    arquivo_corrigido = r['arquivo'].replace('_ocr.txt', '.pdf').replace('.txt', '.pdf')
                    url_ioepa = f"{BASE_URL_IOEPA}{r['ano']}/{arquivo_corrigido}#page={r['pagina']}"

                    tag = f"link_{i}"
                    self.links_armazenados[tag] = url_ioepa
                    
                    idx_inicio = self.text_resultado.index(tk.INSERT)
                    self.text_resultado.insert(tk.END, "ABRIR PDF NO NAVEGADOR\n")
                    idx_fim = self.text_resultado.index(tk.INSERT)
                    
                    self.text_resultado.tag_add("link", idx_inicio, idx_fim)
                    self.text_resultado.tag_add(tag, idx_inicio, idx_fim)
                    self.text_resultado.insert(tk.END, "-" * 60 + "\n")
                
                aviso_limite = "+" if len(res) >= 300 else ""
                if aviso_limite:
                    self.text_resultado.insert(tk.END, "\n   [Aviso: Limite de 300 resultados atingido. Refine sua busca com filtros.]\n")

            self.text_resultado.config(state=tk.DISABLED)
            self.lbl_contador.config(text=f"Resultados ({len(res)}{aviso_limite})")
            self.atualizar_status("Busca concluída.", EstiloUI.CORES['sucesso'])
            self.btn_buscar.config(state=tk.NORMAL)
            
        self.root.after(0, _concluir)

    def abrir_link(self, event):
        try:
            tags = self.text_resultado.tag_names(tk.CURRENT)
            if not tags: return
            
            for tag in tags:
                if tag.startswith("link_") and tag in self.links_armazenados:
                    url = self.links_armazenados[tag]
                    try:
                        webbrowser.open(url)
                    except Exception as e:
                        logger.error(f"Falha ao abrir navegador: {e}")
                        messagebox.showerror("Erro", f"Não foi possível abrir o link: {e}")
                    break
        except Exception as e:
            logger.error(f"Erro em abrir_link: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = BDOEApp(root)
    root.eval('tk::PlaceWindow . center')
    root.mainloop()