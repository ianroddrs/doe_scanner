import os
import re
import tempfile
import requests
from bs4 import BeautifulSoup
import fitz  # PyMuPDF

# Configurações
BASE_URL = "https://www.ioepa.com.br/arquivos/"
PASTA_OCR_LOCAL = "textos_ocr"

def normalizar_texto(texto):
    if not texto: return ""
    texto = texto.lower()
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()

def normalizar_cpf(cpf):
    if not cpf: return ""
    return re.sub(r'\D', '', cpf)

# ==========================================
# BUSCA LOCAL (ANOS 1980 - 2007)
# ==========================================
def buscar_local_txt(nome, ano, mes, dia=None, cpf=None):
    nome_norm = normalizar_texto(nome)
    cpf_norm = normalizar_cpf(cpf)
    
    mes_str = str(mes).zfill(2)
    dia_str = str(dia).zfill(2) if dia else ""
    
    resultados = []
    
    if not os.path.exists(PASTA_OCR_LOCAL):
        print(f"[!] Pasta '{PASTA_OCR_LOCAL}' não encontrada. Rode o script 02 primeiro.")
        return resultados

    # Procura arquivos que correspondam ao padrão YYYY.MM...
    prefixo_busca = f"{ano}.{mes_str}."
    if dia_str:
        prefixo_busca = f"{ano}.{mes_str}.{dia_str}"

    arquivos_alvo = [f for f in os.listdir(PASTA_OCR_LOCAL) if f.startswith(prefixo_busca) and f.endswith('_ocr.txt')]
    
    if not arquivos_alvo:
        print("[-] Nenhum arquivo local processado encontrado para essa data.")
        return resultados
        
    for arquivo in arquivos_alvo:
        caminho_txt = os.path.join(PASTA_OCR_LOCAL, arquivo)
        
        with open(caminho_txt, 'r', encoding='utf-8') as f:
            conteudo = f.read()
            conteudo_norm = normalizar_texto(conteudo)
            
            achou_nome = nome_norm in conteudo_norm
            achou_cpf = True
            if cpf_norm:
                achou_cpf = cpf_norm in normalizar_texto(conteudo)
                
            foi_encontrado = achou_nome and achou_cpf
            
            # Recriando a data original a partir do nome do arquivo
            partes = arquivo.split('.')
            data_formatada = f"{partes[2]}/{partes[1]}/{partes[0]}"
            
            status = "ENCONTRADO" if foi_encontrado else "Não encontrado"
            print(f"  -> {data_formatada} (Local): {status}")
            
            resultados.append({
                'data': data_formatada,
                'origem': 'Local (OCR.txt)',
                'encontrado': foi_encontrado
            })
            
    return resultados

# ==========================================
# BUSCA ONLINE NATIVA (ANOS >= 2008)
# ==========================================
def buscar_online_nativo(nome, ano, mes, dia=None, cpf=None):
    nome_norm = normalizar_texto(nome)
    cpf_norm = normalizar_cpf(cpf)
    
    url_ano = f"{BASE_URL}{ano}/"
    try:
        response = requests.get(url_ano)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        print("[!] Erro ao acessar a página do IOEPA.")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    mes_str = str(mes).zfill(2)
    dia_str = str(dia).zfill(2) if dia else None

    resultados = []

    for a_tag in soup.find_all('a'):
        href = a_tag.get('href')
        if not (href and href.endswith('.pdf')): continue
        
        partes = href.split('/')[-1].split('.')
        if len(partes) < 3: continue
        
        arq_ano, arq_mes, arq_dia = partes[0], partes[1], partes[2]
        
        if arq_ano == str(ano) and arq_mes == mes_str:
            if dia_str and arq_dia != dia_str: continue
            
            url_pdf = href if href.startswith('http') else f"{url_ano}{href.split('/')[-1]}"
            data_formatada = f"{arq_dia}/{arq_mes}/{arq_ano}"
            
            foi_encontrado = False
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
                try:
                    res_pdf = requests.get(url_pdf, stream=True)
                    for chunk in res_pdf.iter_content(8192): temp_pdf.write(chunk)
                    temp_pdf.flush()
                    
                    doc = fitz.open(temp_pdf.name)
                    for pagina in doc:
                        txt_extraido = pagina.get_text()
                        if nome_norm in normalizar_texto(txt_extraido):
                            if not cpf_norm or (cpf_norm in normalizar_texto(txt_extraido)):
                                foi_encontrado = True
                                break
                    doc.close()
                except Exception as e:
                    print(f"  [!] Erro ao processar PDF online: {e}")
                finally:
                    temp_pdf.close()
                    if os.path.exists(temp_pdf.name):
                        try: os.remove(temp_pdf.name)
                        except: pass
            
            status = "ENCONTRADO" if foi_encontrado else "Não encontrado"
            print(f"  -> {data_formatada} (Online Nativo): {status}")
            
            resultados.append({
                'data': data_formatada,
                'origem': 'Online (PyMuPDF)',
                'encontrado': foi_encontrado
            })
            
    return resultados

# ==========================================
# ORQUESTRADOR PRINCIPAL
# ==========================================
def realizar_busca(nome, ano, mes, dia=None, cpf=None):
    print(f"\n--- INICIANDO BUSCA HÍBRIDA ---")
    print(f"Alvo: {nome}")
    print(f"Período: {dia if dia else 'Mês'}/{mes:02d}/{ano}\n")

    if ano <= 2007:
        print("[*] Ano <= 2007 detectado. Buscando nos arquivos OCR locais (Ultra Rápido)...")
        resultados = buscar_local_txt(nome, ano, mes, dia, cpf)
    else:
        print("[*] Ano >= 2008 detectado. Buscando e extraindo texto digital online...")
        resultados = buscar_online_nativo(nome, ano, mes, dia, cpf)

    print("\n--- RESUMO ---")
    encontrados = sum(1 for r in resultados if r['encontrado'])
    print(f"Dias pesquisados: {len(resultados)}")
    print(f"Ocorrências encontradas: {encontrados}")
    return resultados

if __name__ == "__main__":
    # Teste para ano <= 2007 (Vai procurar nos arquivos de texto na sua máquina)
    realizar_busca(nome="LILIAN GREYCE DE ALENCAR SOUZA", ano=2006, mes=12, dia=None)
    
    # Teste para ano >= 2008 (Vai baixar temporariamente e ler o texto digital)
    # realizar_busca(nome="JOAO DA SILVA", ano=2015, mes=5, dia=10)