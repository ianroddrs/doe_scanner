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
from tkinter import ttk, scrolledtext
import time
from threading import Thread
import sys

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CAMINHO_POPPLER = os.path.join(BASE_DIR, 'poppler', 'Library', 'bin')
BASE_URL_IOEPA = "https://www.ioepa.com.br/arquivos/"
DB_FILE = os.path.join(BASE_DIR, "indice_doe.db")

def normalizar_texto(texto):
    if not texto: return ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto = texto.lower()
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()

def extrair_data_nome(nome_arquivo):
    try:
        base = nome_arquivo.upper().replace('.DOE.PDF', '').replace('.PDF', '')
        partes = base.split('.')
        if len(partes) >= 3:
            ano, mes, dia = partes[0], partes[1], partes[2]
            return f"{dia}/{mes}/{ano}", int(ano), int(mes), int(dia)
    except:
        pass
    return None, None, None, None

class IndiceSQLite:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self.db_lock = threading.Lock()
        self._inicializar_tabelas()

    def conectar(self):
        return sqlite3.connect(self.db_path, timeout=60.0)

    def _inicializar_tabelas(self):
        with self.db_lock:
            conn = self.conectar()
            conn.execute("PRAGMA journal_mode=WAL;")
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA table_info(controle_downloads)")
            colunas = cursor.fetchall()
            
            precisa_migrar = False
            if colunas:
                primeira_coluna = colunas[0][1]
                if primeira_coluna == "data_publicacao":
                    precisa_migrar = True
            
            if precisa_migrar:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS controle_downloads_novo (
                        arquivo TEXT PRIMARY KEY,
                        data_publicacao DATE
                    )
                ''')
                cursor.execute('''
                    INSERT OR IGNORE INTO controle_downloads_novo (arquivo, data_publicacao)
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
            conn.close()

    def obter_datas_baixadas(self):
        conn = self.conectar()
        cursor = conn.cursor()
        cursor.execute("SELECT arquivo FROM controle_downloads")
        arquivos = set(row[0] for row in cursor.fetchall())
        conn.close()
        return arquivos

    def salvar_paginas_db(self, paginas_extraidas, arquivo, data_str):
        with self.db_lock:
            for tentativa in range(3):
                try:
                    conn = self.conectar()
                    cursor = conn.cursor()
                    
                    if paginas_extraidas:
                        for pag in paginas_extraidas:
                            cursor.execute('''
                                INSERT INTO diarios (arquivo, data, ano, mes, dia, pagina, texto)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (pag['arquivo'], pag['data'], pag['ano'], pag['mes'], pag['dia'], pag['pagina'], pag['texto']))
                    
                    if data_str:
                        data_db = datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
                        cursor.execute("INSERT OR IGNORE INTO controle_downloads (arquivo, data_publicacao) VALUES (?, ?)", (arquivo, data_db))
                    
                    conn.commit()
                    conn.close()
                    break
                    
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower():
                        time.sleep(1)
                    else:
                        print(f"Erro DB ao salvar {arquivo}: {e}")
                        break
                except Exception as e:
                    print(f"Erro geral DB em {arquivo}: {e}")
                    break

    def buscar(self, nome, cpf=None, ano=None, mes=None, dia=None):
        nome_norm = normalizar_texto(nome)
        cpf_norm = re.sub(r'\D', '', cpf) if cpf else None
        if not nome_norm: return []

        conn = self.conectar()
        cursor = conn.cursor()
        query_match = f'"{nome_norm}"'
        if cpf_norm: query_match += f' AND "{cpf_norm}"'

        sql = "SELECT arquivo, data, ano, pagina FROM diarios WHERE diarios MATCH ?"
        params = [query_match]

        if ano: sql += " AND ano = ?"; params.append(int(ano))
        if mes: sql += " AND mes = ?"; params.append(int(mes))
        if dia: sql += " AND dia = ?"; params.append(int(dia))

        try:
            cursor.execute(sql, params)
            linhas = cursor.fetchall()
        except:
            linhas = []
        conn.close()

        resultados = [{'arquivo': l[0], 'data': l[1], 'ano': l[2], 'pagina': l[3]} for l in linhas]
        try:
            resultados.sort(key=lambda x: datetime.strptime(x['data'], '%d/%m/%Y'), reverse=True)
        except: pass
        return resultados

class AtualizadorDOE:
    def __init__(self, db, callback_status=None, callback_progresso=None):
        self.db = db
        self.callback_status = callback_status
        self.callback_progresso = callback_progresso
        self.arquivos_ja_baixados = self.db.obter_datas_baixadas()

    def buscar_links_disponiveis(self):
        self.arquivos_ja_baixados = self.db.obter_datas_baixadas()
        links_para_baixar = []
        ano_atual = datetime.now().year
        
        try:
            url_ano = f"{BASE_URL_IOEPA}{ano_atual}/"
            resposta = requests.get(url_ano, timeout=10)
            soup = BeautifulSoup(resposta.text, 'html.parser')
            
            for a_tag in soup.find_all('a'):
                href = a_tag.get('href')
                if href and href.lower().endswith('.pdf'):
                    nome_arquivo = href.split('/')[-1]
                    if nome_arquivo not in self.arquivos_ja_baixados:
                        url_completa = href if href.startswith('http') else f"{url_ano}{nome_arquivo}"
                        links_para_baixar.append({'url': url_completa, 'arquivo': nome_arquivo})
        except Exception as e:
            if self.callback_status: self.callback_status(f"Erro rede: {e}", "#EF4444")
            
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
                resposta = requests.get(url, stream=True, timeout=30)
                resposta.raise_for_status()
                for chunk in resposta.iter_content(chunk_size=8192):
                    temp_pdf.write(chunk)
                temp_pdf.flush()
                
                texto = normalizar_texto(pagina.get_text())
                if not texto:
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
                            
                self.db.salvar_paginas_db(paginas_extraidas, arquivo, data_str)
                return True
            except Exception as e:
                print(f"Erro arquivo {arquivo}: {e}")
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            futuros = {executor.submit(self.processar_pdf_download, link): link for link in links}
            for futuro in concurrent.futures.as_completed(futuros):
                concluidos += 1
                if self.callback_progresso: self.callback_progresso(concluidos, total)

        # Otimiza o banco garantindo que usa a trava para não colidir
        try:
            with self.db.db_lock:
                conn = self.db.conectar()
                conn.execute("INSERT INTO diarios(diarios) VALUES('optimize')")
                conn.commit()
                conn.close()
        except Exception as e:
            print(f"Erro na otimização final: {e}")

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

class BDOEApp:
    def __init__(self, root):
        self.root = root
        self.root.title("BDOE - Polícia Civil")
        self.root.geometry("850x650")
        self.root.minsize(800, 600)
        self.root.configure(bg=EstiloUI.CORES['fundo_app'])
        
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

        tk.Label(inner_busca, text="Filtros Data (Dia / Mês / Ano):", font=EstiloUI.FONTES['label'], bg=EstiloUI.CORES['fundo_card']).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=(5, 2))
        frame_datas = tk.Frame(inner_busca, bg=EstiloUI.CORES['fundo_card'])
        frame_datas.grid(row=3, column=0, columnspan=2, sticky=tk.W)
        
        self.entry_dia = tk.Entry(frame_datas, font=EstiloUI.FONTES['input'], width=4, justify='center', relief=tk.FLAT, highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1)
        self.entry_dia.pack(side=tk.LEFT, padx=(0, 5), ipady=3)
        self.entry_mes = tk.Entry(frame_datas, font=EstiloUI.FONTES['input'], width=4, justify='center', relief=tk.FLAT, highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1)
        self.entry_mes.pack(side=tk.LEFT, padx=(0, 5), ipady=3)
        self.entry_ano = tk.Entry(frame_datas, font=EstiloUI.FONTES['input'], width=6, justify='center', relief=tk.FLAT, highlightbackground=EstiloUI.CORES['borda'], highlightthickness=1)
        self.entry_ano.pack(side=tk.LEFT, padx=(0, 10), ipady=3)

        frame_botoes = tk.Frame(inner_busca, bg=EstiloUI.CORES['fundo_card'])
        frame_botoes.grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=(15, 0))

        self.btn_buscar = tk.Button(frame_botoes, text="🔍 BUSCAR", font=EstiloUI.FONTES['botao'], bg=EstiloUI.CORES['principal'], fg="#FFFFFF", relief=tk.FLAT, padx=15, pady=6, command=self.iniciar_busca)
        self.btn_buscar.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_atualizar = tk.Button(frame_botoes, text="⬇️ ATUALIZAR", font=EstiloUI.FONTES['botao'], bg=EstiloUI.CORES['alerta'], fg="#FFFFFF", relief=tk.FLAT, padx=15, pady=6, command=self.acionar_atualizacao)
        self.btn_atualizar.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_limpar = tk.Button(frame_botoes, text="LIMPAR", font=EstiloUI.FONTES['botao'], bg=EstiloUI.CORES['botao_secundario'], fg="#FFFFFF", relief=tk.FLAT, padx=15, pady=6, command=self.limpar_campos)
        self.btn_limpar.pack(side=tk.LEFT)

        for entry in [self.entry_nome, self.entry_cpf, self.entry_dia, self.entry_mes, self.entry_ano]:
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
            atualizador.iniciar_atualizacao()
            self.root.after(0, lambda: self.btn_atualizar.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.btn_buscar.config(state=tk.NORMAL))
        Thread(target=_thread, daemon=True).start()

    def limpar_campos(self):
        for entry in [self.entry_nome, self.entry_cpf, self.entry_dia, self.entry_mes, self.entry_ano]:
            entry.delete(0, tk.END)
        self.text_resultado.config(state=tk.NORMAL)
        self.text_resultado.delete(1.0, tk.END)
        self.text_resultado.config(state=tk.DISABLED)
        self.lbl_contador.config(text="Resultados (0)")

    def iniciar_busca(self):
        nome = self.entry_nome.get().strip()
        if len(nome) < 2: return

        self.btn_buscar.config(state=tk.DISABLED)
        self.atualizar_status("Buscando...", EstiloUI.CORES['alerta'], True, 'indeterminate')
        self.text_resultado.config(state=tk.NORMAL)
        self.text_resultado.delete(1.0, tk.END)
        self.links_armazenados.clear()
        
        Thread(target=self._executar, args=(nome,), daemon=True).start()

    def _executar(self, nome):
        res = self.db.buscar(nome, self.entry_cpf.get(), self.entry_ano.get(), self.entry_mes.get(), self.entry_dia.get())

        def _concluir():
            if not res:
                self.text_resultado.insert(tk.END, "\n   Nenhum registro encontrado.")
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

            self.text_resultado.config(state=tk.DISABLED)
            self.lbl_contador.config(text=f"Resultados ({len(res)})")
            self.atualizar_status("Busca concluída.", EstiloUI.CORES['sucesso'])
            self.btn_buscar.config(state=tk.NORMAL)
        self.root.after(0, _concluir)

    def abrir_link(self, event):
        tags = self.text_resultado.tag_names(tk.CURRENT)
        for tag in tags:
            if tag.startswith("link_") and tag in self.links_armazenados:
                webbrowser.open(self.links_armazenados[tag])
                break

if __name__ == "__main__":
    root = tk.Tk()
    app = BDOEApp(root)
    root.eval('tk::PlaceWindow . center')
    root.mainloop()