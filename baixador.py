import os
import requests
from bs4 import BeautifulSoup

# Configurações
ANO_INICIO = 1980
ANO_FIM = 2007
BASE_URL = "https://www.ioepa.com.br/arquivos/"
PASTA_DESTINO = "pdfs_originais"

def iniciar_download():
    # Cria a pasta se não existir
    if not os.path.exists(PASTA_DESTINO):
        os.makedirs(PASTA_DESTINO)
        print(f"[*] Pasta '{PASTA_DESTINO}' criada.")

    for ano in range(ANO_INICIO, ANO_FIM + 1):
        url_ano = f"{BASE_URL}{ano}/"
        print(f"\n================================")
        print(f"[*] Acessando diretório do ano: {ano}")
        print(f"================================")
        
        try:
            response = requests.get(url_ano)
            # Se a página não existir (ex: ano sem registros), pula para o próximo
            if response.status_code != 200:
                print(f"[!] Diretório do ano {ano} não encontrado no servidor.")
                continue
        except Exception as e:
            print(f"[!] Erro ao conectar no ano {ano}: {e}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        
        pdfs_para_baixar = [a.get('href') for a in links if a.get('href') and a.get('href').endswith('.pdf')]
        
        if not pdfs_para_baixar:
            print(f"[-] Nenhum PDF encontrado para o ano {ano}.")
            continue
            
        print(f"[*] Encontrados {len(pdfs_para_baixar)} PDFs em {ano}. Iniciando download...")

        for href in pdfs_para_baixar:
            nome_arquivo = href.split('/')[-1]
            caminho_local = os.path.join(PASTA_DESTINO, nome_arquivo)
            
            # Pula o arquivo se ele já existir (ótimo para continuar downloads interrompidos)
            if os.path.exists(caminho_local):
                print(f"    -> [PULANDO] {nome_arquivo} já existe.")
                continue

            # Monta a URL completa se for relativa
            url_completa = href if href.startswith('http') else f"{url_ano}{nome_arquivo}"
            
            print(f"    -> Baixando {nome_arquivo}...")
            try:
                resposta_pdf = requests.get(url_completa, stream=True)
                resposta_pdf.raise_for_status()
                
                with open(caminho_local, 'wb') as f:
                    for chunk in resposta_pdf.iter_content(chunk_size=8192):
                        f.write(chunk)
            except Exception as e:
                print(f"    [!] Erro ao baixar {nome_arquivo}: {e}")

if __name__ == "__main__":
    print("INICIANDO DOWNLOADER EM LOTE - IOEPA")
    iniciar_download()
    print("\n[OK] Processo de download finalizado!")