import os
import re
import tempfile
import requests
from bs4 import BeautifulSoup
import fitz  # PyMuPDF

# ==========================================
# CONFIGURAÇÕES
# ==========================================
BASE_URL = "https://www.ioepa.com.br/arquivos/"
PASTA_OCR_LOCAL = "textos_ocr"

# Intervalos de busca caso não sejam informados
ANOS_LOCAL = range(1980, 2008)  # Para arquivos TXT locais
ANOS_ONLINE = range(2008, 2027) # Para PDFs online (ajuste conforme o ano atual)

def normalizar_texto(texto):
    if not texto: return ""
    texto = texto.lower()
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()

def normalizar_cpf(cpf):
    if not cpf: return ""
    return re.sub(r'\D', '', cpf)

# ==========================================
# BUSCA LOCAL (TXTs)
# ==========================================
def buscar_local_txt(nome, ano=None, mes=None, dia=None, cpf=None):
    nome_norm = normalizar_texto(nome)
    cpf_norm = normalizar_cpf(cpf)
    resultados = []

    if not os.path.exists(PASTA_OCR_LOCAL):
        return resultados

    # Cria o padrão de busca (Ex: "1980.03.15" ou "1980.03" ou "1980")
    partes_prefixo = []
    if ano: partes_prefixo.append(str(ano))
    if mes: partes_prefixo.append(str(mes).zfill(2))
    if dia: partes_prefixo.append(str(dia).zfill(2))
    
    prefixo_busca = ".".join(partes_prefixo)

    arquivos_alvo = [f for f in os.listdir(PASTA_OCR_LOCAL) 
                     if f.startswith(prefixo_busca) and f.endswith('_ocr.txt')]
    
    for arquivo in arquivos_alvo:
        caminho_txt = os.path.join(PASTA_OCR_LOCAL, arquivo)
        with open(caminho_txt, 'r', encoding='utf-8') as f:
            conteudo_norm = normalizar_texto(f.read())
            
            achou_nome = nome_norm in conteudo_norm
            achou_cpf = True if not cpf_norm else (cpf_norm in conteudo_norm)
            
            if achou_nome and achou_cpf:
                p = arquivo.split('.')
                data_fmt = f"{p[2]}/{p[1]}/{p[0]}"
                print(f"  [+] Encontrado Local: {data_fmt}")
                resultados.append({'data': data_fmt, 'origem': 'Local', 'encontrado': True})
    return resultados

# ==========================================
# BUSCA ONLINE (PDFs)
# ==========================================
def buscar_online_nativo(nome, ano=None, mes=None, dia=None, cpf=None):
    nome_norm = normalizar_texto(nome)
    cpf_norm = normalizar_cpf(cpf)
    resultados = []
    
    # Se o ano não for informado, percorre o range definido
    anos_para_buscar = [ano] if ano else ANOS_ONLINE
    mes_alvo = str(mes).zfill(2) if mes else None
    dia_alvo = str(dia).zfill(2) if dia else None

    for a in anos_para_buscar:
        url_ano = f"{BASE_URL}{a}/"
        try:
            response = requests.get(url_ano, timeout=10)
            if response.status_code != 200: continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            links = [a_tag.get('href') for a_tag in soup.find_all('a') if a_tag.get('href', '').endswith('.pdf')]
            
            for href in links:
                # O padrão esperado é YYYY.MM.DD.pdf
                partes = href.split('/')[-1].split('.')
                if len(partes) < 3: continue
                
                arq_ano, arq_mes, arq_dia = partes[0], partes[1], partes[2]
                
                # Filtros opcionais
                if mes_alvo and arq_mes != mes_alvo: continue
                if dia_alvo and arq_dia != dia_alvo: continue
                
                url_pdf = href if href.startswith('http') else f"{url_ano}{href.split('/')[-1]}"
                
                # Processamento do PDF (Leitura em memória/temp)
                foi_encontrado = False
                try:
                    res_pdf = requests.get(url_pdf, timeout=15)
                    with fitz.open(stream=res_pdf.content, filetype="pdf") as doc:
                        for pagina in doc:
                            txt_extraido = normalizar_texto(pagina.get_text())
                            if nome_norm in txt_extraido:
                                if not cpf_norm or (cpf_norm in txt_extraido):
                                    foi_encontrado = True
                                    break
                except: continue

                if foi_encontrado:
                    data_fmt = f"{arq_dia}/{arq_mes}/{arq_ano}"
                    print(f"  [+] Encontrado Online: {data_fmt}")
                    resultados.append({'data': data_fmt, 'origem': 'Online', 'encontrado': True})
                    
        except: continue
        
    return resultados

# ==========================================
# ORQUESTRADOR
# ==========================================
def realizar_busca(nome, ano=None, mes=None, dia=None, cpf=None):
    print(f"\n{'='*40}")
    print(f"BUSCA HÍBRIDA: {nome}")
    print(f"FILTROS: Ano={ano or 'Todos'}, Mês={mes or 'Todos'}, Dia={dia or 'Todos'}")
    print(f"{'='*40}\n")

    todos_resultados = []

    # Decide onde buscar baseado no ano informado ou faz busca total
    if ano:
        if ano <= 2007:
            todos_resultados.extend(buscar_local_txt(nome, ano, mes, dia, cpf))
        else:
            todos_resultados.extend(buscar_online_nativo(nome, ano, mes, dia, cpf))
    else:
        # Busca em tudo (Cuidado: Isso pode demorar muito online!)
        print("[*] Buscando em arquivos locais (1980-2007)...")
        todos_resultados.extend(buscar_local_txt(nome, None, mes, dia, cpf))
        print("[*] Buscando em arquivos online (2008+)...")
        todos_resultados.extend(buscar_online_nativo(nome, None, mes, dia, cpf))

    print(f"\n--- RESUMO FINAL ---")
    print(f"Total de ocorrências: {len(todos_resultados)}")
    return todos_resultados

if __name__ == "__main__":
    # EXEMPLOS DE USO:
    
    # 1. Busca específica
    # realizar_busca(nome="IAN MATEUS ALVES RODRIGUES", ano=2025, mes=5)

    # 2. Busca apenas por nome em todos os anos (Demorado se houver muitos PDFs)
    # realizar_busca(nome="NOME DO ALVO")

    # 3. Busca por nome e dia específico, independente do mês ou ano
    realizar_busca("maria da conceição marques pinto")