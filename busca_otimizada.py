import os
import re
import sqlite3
import unicodedata
from datetime import datetime

PASTA_OCR_LOCAL = "textos_ocr"
DB_FILE = "indice_doe.db"

def normalizar_texto(texto):
    if not texto: return ""
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto = texto.lower()
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()

def extrair_data_doe(nome_arquivo):
    try:
        nome_sem_ext = nome_arquivo.replace('_ocr.txt', '')
        partes = nome_sem_ext.split('.')
        if len(partes) >= 3:
            ano, mes, dia = partes[0], partes[1], partes[2]
            return f"{dia}/{mes}/{ano}", int(ano), int(mes), int(dia)
    except:
        pass
    return None, None, None, None

class IndiceSQLite:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self.indice_pronto = self.verificar_db()

    def verificar_db(self):
        if not os.path.exists(self.db_path): 
            return False
        
        if os.path.getsize(self.db_path) < 1000000:
            return False

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='diarios'")
            tem_tabela = cursor.fetchone() is not None
            conn.close()
            return tem_tabela
        except:
            return False

    def conectar(self, modo_construcao=False):
        conn = sqlite3.connect(self.db_path)
        if modo_construcao:
            conn.execute("PRAGMA journal_mode=OFF;")
            conn.execute("PRAGMA synchronous=OFF;")
        else:
            conn.execute("PRAGMA journal_mode=DELETE;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-64000;")
        return conn

    def construir_indice(self, progress_callback=None):
        if not os.path.exists(PASTA_OCR_LOCAL):
            return False

        for ext in ['', '-wal', '-shm']:
            try:
                if os.path.exists(self.db_path + ext):
                    os.remove(self.db_path + ext)
            except: pass

        conn = self.conectar(modo_construcao=True)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE VIRTUAL TABLE diarios USING fts5(
                arquivo, data, ano UNINDEXED, mes UNINDEXED, dia UNINDEXED, pagina UNINDEXED, texto,
                tokenize='unicode61 remove_diacritics 1'
            )
        ''')

        arquivos = [f for f in os.listdir(PASTA_OCR_LOCAL) if f.endswith('.txt')]
        total = len(arquivos)

        for i, arquivo in enumerate(arquivos):
            if (i + 1) % 100 == 0 or i == 0 or (i + 1) == total:
                if progress_callback:
                    progress_callback(i + 1, total)

            caminho = os.path.join(PASTA_OCR_LOCAL, arquivo)
            
            conteudo = None
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    with open(caminho, 'r', encoding=encoding) as f:
                        conteudo = f.read()
                        break
                except:
                    pass
                    
            if not conteudo: continue

            data_fmt, ano, mes, dia = extrair_data_doe(arquivo)
            if not data_fmt: continue

            separadores = ['\f', '---PAGE---', '---page---', '<<PAGE>>', '\n\f\n']
            paginas = None
            for sep in separadores:
                if sep in conteudo:
                    paginas = conteudo.split(sep)
                    break
            if not paginas:
                paginas = re.split(r'\n\n\n+', conteudo)

            for num_pagina, texto_pag in enumerate(paginas, 1):
                texto_norm = normalizar_texto(texto_pag)
                if not texto_norm: continue
                
                cursor.execute('''
                    INSERT INTO diarios (arquivo, data, ano, mes, dia, pagina, texto)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (arquivo, data_fmt, ano, mes, dia, str(num_pagina), texto_norm))

            if (i + 1) % 100 == 0:
                conn.commit()

        conn.commit()
        
        cursor.execute("INSERT INTO diarios(diarios) VALUES('optimize')")
        conn.commit()
        conn.close()

        self.indice_pronto = True
        return True

    def buscar(self, nome, cpf=None, ano=None, mes=None, dia=None):
        if not self.indice_pronto:
            return []

        nome_norm = normalizar_texto(nome)
        cpf_norm = re.sub(r'\D', '', cpf) if cpf else None

        if not nome_norm: return []

        conn = self.conectar(modo_construcao=False)
        cursor = conn.cursor()

        query_match = f'"{nome_norm}"'
        
        if cpf_norm:
            query_match += f' AND "{cpf_norm}"'

        sql = "SELECT arquivo, data, pagina FROM diarios WHERE diarios MATCH ?"
        params = [query_match]

        if ano and ano.isdigit():
            sql += " AND ano = ?"
            params.append(int(ano))
        if mes:
            sql += " AND mes = ?"
            params.append(str(mes).zfill(2))
        if dia:
            sql += " AND dia = ?"
            params.append(str(dia).zfill(2))

        try:
            cursor.execute(sql, params)
            linhas = cursor.fetchall()
        except:
            linhas = []

        conn.close()

        resultados = [
            {'data': l[1], 'arquivo': l[0], 'pagina': l[2], 'encontrado': True}
            for l in linhas
        ]

        try:
            resultados.sort(
                key=lambda x: datetime.strptime(x['data'], '%d/%m/%Y'),
                reverse=True
            )
        except:
            pass

        return resultados

_indice_global = None

def inicializar_indice(progress_callback=None):
    global _indice_global
    if _indice_global is None:
        _indice_global = IndiceSQLite()
        if not _indice_global.indice_pronto:
            _indice_global.construir_indice(progress_callback)

def buscar_local_txt_otimizado(nome, cpf=None, ano=None, mes=None, dia=None):
    global _indice_global
    if _indice_global is None:
        inicializar_indice()
    if not _indice_global.indice_pronto:
        return []
    return _indice_global.buscar(nome, cpf, ano, mes, dia)