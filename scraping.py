import os
import re
import tempfile
import requests
from bs4 import BeautifulSoup
from pdf2image import convert_from_path
import pytesseract
import fitz  # Importação da biblioteca PyMuPDF (pip install PyMuPDF)

# ==========================================
# CONFIGURAÇÕES E PRÉ-REQUISITOS
# ==========================================
# 1. Instalar bibliotecas Python:
#    pip install requests beautifulsoup4 pdf2image pytesseract PyMuPDF
#
# 2. Instalar programas no seu Sistema Operacional para o OCR (< 2008):
#    - Tesseract OCR
#    - Poppler
# ==========================================

# Ajuste aqui com o seu caminho do Poppler e Tesseract se necessário:
CAMINHO_POPPLER = r'poppler\Library\bin' 
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

BASE_URL = "https://www.ioepa.com.br/arquivos/"

def normalizar_texto(texto):
    """
    Remove pontuações, acentos e converte para minúsculas.
    """
    if not texto:
        return ""
    texto = texto.lower()
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def normalizar_cpf(cpf):
    """Remove pontos e traços do CPF."""
    if not cpf:
        return ""
    return re.sub(r'\D', '', cpf)

def obter_links_pdfs(ano, mes, dia=None):
    """
    Acessa a pasta do ano e retorna uma lista de URLs de PDFs do mês e dia específicos.
    """
    url_ano = f"{BASE_URL}{ano}/"
    print(f"[*] Acessando diretório: {url_ano}")
    
    try:
        response = requests.get(url_ano)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[!] Erro ao acessar a página do ano {ano}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links_pdfs = []
    
    mes_str = str(mes).zfill(2)
    dia_str = str(dia).zfill(2) if dia else None

    for a_tag in soup.find_all('a'):
        href = a_tag.get('href')
        if href and href.endswith('.pdf'):
            nome_arquivo = href.split('/')[-1]
            partes = nome_arquivo.split('.')
            
            if len(partes) >= 3:
                arquivo_ano, arquivo_mes, arquivo_dia = partes[0], partes[1], partes[2]
                
                if arquivo_ano == str(ano) and arquivo_mes == mes_str:
                    if dia_str and arquivo_dia != dia_str:
                        continue

                    if not href.startswith('http'):
                        href = f"{url_ano}{nome_arquivo}"
                    
                    data_formatada = f"{arquivo_dia}/{arquivo_mes}/{arquivo_ano}"
                    links_pdfs.append({
                        'data': data_formatada,
                        'url': href,
                        'arquivo': nome_arquivo
                    })
                    
    return sorted(links_pdfs, key=lambda x: x['data'])

def processar_pdf(pdf_url, nome_alvo, cpf_alvo=None, ano_pdf=None):
    """
    Faz o download do PDF e decide se usa Extração Nativa (>=2008) ou OCR (<2008).
    """
    nome_alvo_norm = normalizar_texto(nome_alvo)
    cpf_alvo_norm = normalizar_cpf(cpf_alvo)
    
    encontrado = False
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
        temp_pdf_path = temp_pdf.name
        try:
            # 1. Download do PDF (Necessário para ambos os métodos)
            resposta = requests.get(pdf_url, stream=True)
            resposta.raise_for_status()
            for chunk in resposta.iter_content(chunk_size=8192):
                temp_pdf.write(chunk)
            temp_pdf.flush() 
            
            # 2. Lógica Baseada no Ano
            if ano_pdf >= 2008:
                print("      -> Usando extração de texto nativo (Rápido)")
                doc = fitz.open(temp_pdf_path)
                
                for num_pagina, pagina in enumerate(doc, start=1):
                    texto_extraido = pagina.get_text()
                    texto_norm = normalizar_texto(texto_extraido)
                    
                    achou_nome = nome_alvo_norm in texto_norm
                    achou_cpf = True
                    
                    if cpf_alvo_norm:
                        achou_cpf = cpf_alvo_norm in normalizar_texto(texto_extraido)
                    
                    if achou_nome and achou_cpf:
                        encontrado = True
                        break 
                doc.close()

            else:
                print("      -> Usando extração via OCR (Imagens escaneadas)")
                # OCR Otimizado: tons de cinza, multi-thread e DPI menor
                paginas = convert_from_path(
                    temp_pdf_path, 
                    dpi=150, 
                    poppler_path=CAMINHO_POPPLER,
                    grayscale=True,
                    thread_count=4
                )
                
                for num_pagina, imagem_pagina in enumerate(paginas, start=1):
                    texto_extraido = pytesseract.image_to_string(imagem_pagina, lang='por')
                    texto_norm = normalizar_texto(texto_extraido)
                    
                    achou_nome = nome_alvo_norm in texto_norm
                    achou_cpf = True
                    
                    if cpf_alvo_norm:
                        achou_cpf = cpf_alvo_norm in normalizar_texto(texto_extraido) 
                    
                    if achou_nome and achou_cpf:
                        encontrado = True
                        break 

        except Exception as e:
            print(f"  [!] Erro ao processar o PDF {pdf_url}: {e}")
            return False
            
        finally:
            temp_pdf.close()
            if os.path.exists(temp_pdf_path):
                try:
                    os.remove(temp_pdf_path)
                except PermissionError:
                    pass # Evita travar se o arquivo ainda estiver preso no Windows
                
    return encontrado

def buscar_no_diario(nome, mes, ano, dia=None, cpf=None):
    """
    Função principal que orquestra o scraping.
    """
    print(f"\n--- INICIANDO BUSCA ---")
    print(f"Alvo: {nome} | CPF: {cpf if cpf else 'Não informado'}")
    dia_texto = f"{dia:02d}/" if dia else ""
    print(f"Período: {dia_texto}{mes:02d}/{ano}")
    print(f"-----------------------\n")

    pdfs = obter_links_pdfs(ano, mes, dia)
    
    if not pdfs:
        print(f"Nenhum Diário Oficial encontrado para o período.")
        return []

    print(f"[*] Encontrados {len(pdfs)} Diários Oficiais para o período.\n")

    resultados = []

    for pdf in pdfs:
        print(f"[*] Analisando diário do dia {pdf['data']}...")
        
        # Passando o ano para a função decidir o método
        foi_encontrado = processar_pdf(pdf['url'], nome, cpf, ano_pdf=ano)
        
        status = "ENCONTRADO" if foi_encontrado else "Não encontrado"
        print(f"    -> Resultado final: {status}")
        
        resultados.append({
            'data': pdf['data'],
            'url': pdf['url'],
            'encontrado': foi_encontrado
        })

    print("\n--- RESUMO DA BUSCA ---")
    encontrados_total = sum(1 for r in resultados if r['encontrado'])
    print(f"Total de dias pesquisados: {len(resultados)}")
    print(f"Ocorrências encontradas: {encontrados_total}")
    
    return resultados

# ==========================================
# EXECUÇÃO DO SCRIPT
# ==========================================
if __name__ == "__main__":
    # Exemplo de uso
    NOME_BUSCA = "silvestre lima barros"
    CPF_BUSCA = None    
    
    DIA_BUSCA = 6     # Deixe None para buscar o mês inteiro
    MES_BUSCA = 1
    ANO_BUSCA = 2026   # Como é >= 2008, usará a extração nativa super rápida!
    
    relatorio = buscar_no_diario(
        nome=NOME_BUSCA, 
        mes=MES_BUSCA, 
        ano=ANO_BUSCA, 
        dia=DIA_BUSCA,
        cpf=CPF_BUSCA
    )