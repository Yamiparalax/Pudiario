# Introdução

Este script tem como objetivo automatizar o processo de obtenção dos PU's Externos diários nas fontes adequadas. Possíveis fontes são: Sites das securitizadoras, arquivos Excel localizados no diretório da área e arquivos CSV baixados à partir do site da CETIP. Após a execução do script, os resultados são armazenados em um dataframe que então é anexado à planilha de PU externo diário. 

## Funcionamento

### Sites Securitizadoras

1. Deve-se cadastrar os pares `CodAtivo-Site` (separados por **hífen** necessariamente!) nos respectivos arquivos `pu_[nome_securitizadora].txt` presentes nesta pasta.
2. As funções de busca implementadas para o site de cada securitizadora consumirão os respectivos arquivos de entrada e extrairão o PU diário caso ele tenha sido encontrado, caso contrário será informado o erro. 

### Planilhas diretório

1. No arquivo de PU diário, há uma aba chamada "Diretório". Nela, deve-se inserir o código de ativo, a linha contém o cabeçalho da planilha, o número da coluna que contém a data e o número da coluna que contém o PU (Coluna A = 0, Coluna B = 1...).
> Começa-se a contar por 0!

2. Verificar se as planilhas dos ativos a serem consultados estão no formato `.xlsx` caso contrário **não vai funcionar!**

3. Após a execução do Script, os resultados das consultas serão armazenados na aba `Resultados` 

4. Na aba `PU Diário`, fazer o procv usando o código de ativo buscando o PU contido na aba `Resultados`. Colar valores após para remover as fórmulas.

### Arquivos Cetip

1. Na planilha de PU diário, registrar a fonte do arquivo que deve ser utilizado para calcular os PU's (CETIP IMOB | CETIP DEB - Deve ser escrito neste padrão!).

2. Fazer o Download dos respectivos arquivos na CETIP. Não mover os arquivos da pasta `Downloads`, o script procurará nela para encontrar os arquivos.
> Arquivo LIG's: 29590\_[DataAnterior]\_DCUSTODIAPART-IMOB.CETIP21  |   | Arquivo DEB's: 29590\_[DataAnterior]\_DCUSTODIAPART-DEB.CETIP21

3. Mudar a extensão dos arquivos baixados para `.csv` caso contrário **não vai funcionar!**

### CRA's Dólar - NÃO FUNCIONANDO!

1. No arquivo de PU diário, há uma aba chamada "CRA_Dolar".  Nela, deve-se inserir o código de ativo, a linha contém o cabeçalho da planilha, o número da coluna que contém a data, o número da coluna que contém o PU e o número da coluna que receberá o dólar.
> Começa-se a contar por 0!

2. Busca no site referenciado pelo banco central para a cotação do dólar do dia anterior (normalmente o dólar para o dia atual é atualizado **após** a realização do processo).

3. Importa a planilha de precificação dos ativos como um dataframe pandas. Inputa-se o valor do dólar extraído. A partir das fórmulas na própria planilha, extrai-se o PU diário.
> **OBS**: Por ter sido feita a partir de um dataframe, a planilha não guarda as alterações realizadas

## Observações

1. Atentar-se à versão do Driver do navegador, que deve ser armazenado na mesma pasta que contém este script. Caso surja um problema de Driver, baixar a versão mais recente do arquivo `msedgedriver.exe` e substituir a versão antiga.
2. Sites que pedem confirmação sobre cookies via popup podem impedir o carregamento e identificação dos elementos usados para navegar pelas páginas web. Nestes casos, fechar o popup rapidamente para seguir com a execução.

## Responsáveis

1. carlos.lsilva@c6bank.com
2. bruno.loffreda@c6bank.com

# Instalação das dependências - Executar na 1ª vez que o código for rodar em uma máquina nova

Remover o `#` apenas quando quiser que a célula execute de fato
#!pip install pandas selenium numpy openpyxl
# Import das Dependências
import os
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import re
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import openpyxl
# Variáveis Globais
# Criação da lista para armazenar os PU's a fim de criar um dataframe consolidado 
lista_pu_geral = []

# Datas a serem utilizadas:
data_atual_raw = datetime.now()
data_atual_global = data_atual_raw.strftime("%d/%m/%Y")

if(datetime.now().weekday()) == 0:
    data_anterior_raw = datetime.now() - timedelta(days=3)
    data_anterior_global = data_anterior_raw.strftime("%d/%m/%Y") # Segunda Feira 
else:
    data_anterior_raw = datetime.now() - timedelta(days=1) # Demais Dias
    data_anterior_global = data_anterior_raw.strftime("%d/%m/%Y")
# Caminhos

Caso algum caminho seja alterado, refletir as mudanças nesta célula
# Caminho para o diretório com as planilhas de PU
path_dir = os.path.join(f"C:\\Users\\{os.getlogin()}\\C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A\\Back Investimentos - Documentos\\Transferencia custodia\\Atualização PU\\VIRGOPENTAGONO")

# Caminho para a planilha de PU diário
path_pu = f"C:\\Users\\{os.getlogin()}\\C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A\\Back Investimentos - Documentos\\Transferencia custodia\\Atualização PU\\Pu Novo 2025"

# Caminho para a pasta de downloads
path_downloads = f"C:\\Users\\{os.getlogin()}\\Downloads"

# Definição do nome do arquivo da planilha do dia anterior
pu_diario_ontem = path_pu + "\\Pu " + data_anterior_raw.strftime("%d%m") + ".xlsx"
pu_diario_ontem_alt = path_pu + "\\Pu " + data_anterior_raw.strftime("%d%m") + " I.xlsx" # Para os casos em que há erro durante o salvamento

# Definição do nome do arquivo da planilha de PU diário
pu_diario_file = path_pu + "\\Pu " + datetime.today().strftime("%d%m") + ".xlsx"
# Manipulação Inicial dos arquivos de PU
def atualiza_arquivo_pu(arquivo_pu_ontem, arquivo_pu_hoje):
   
    '''
    Função criada para remover a necessidade de copiar, renomear e colar o arquivo de pu diário do dia anterior.
    '''
    # Verifica se o arquivo do dia atual já existe - Evita sobrescrever indevidamente
    if(os.path.isfile(pu_diario_file)):
        print("Arquivo já criado!")
    else:
        print("Arquivo não encontrado... copiando o do dia anterior")
         # Verifica se encontra o arquivo com o nome padrão na pasta e cria uma cópia dele
        if(os.path.isfile(pu_diario_ontem)):
            shutil.copy2(pu_diario_ontem, pu_diario_file)
        # Se não existir, testa com o caminho alternativo
        else:
            shutil.copy2(pu_diario_ontem_alt, pu_diario_file)
    
        # Delay oara garantir a criação do arquivo
        time.sleep(5)
        
        # Verifica se o arquivo foi criado com sucesso
        if(os.path.isfile(pu_diario_file)):
            print("Arquivo copiado com sucesso!")
        else:
            print("Erro ao copiar o arquivo...")
# CÓPIA DA PLANILHA DO DIA ANTERIOR
atualiza_arquivo_pu(pu_diario_ontem,pu_diario_file)
# Formatação dos TXTs
# Função para processar arquivos .txt e substituir espaços por hífens
def substituir_espacos_por_hifen(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Processar cada linha e substituir espaços por hífens
    updated_lines = []
    for line in lines:
        updated_line = line.strip().replace('\t', '-').replace(' ', '-')
        updated_lines.append(updated_line)

    # Escrever o conteúdo atualizado no arquivo
    with open(filename, 'w', encoding='utf-8') as file:
        for line in updated_lines:
            file.write(line + '\n')

    print(f"Arquivo '{filename}' processado com sucesso.")

# Caminho do diretório atual
current_directory = os.getcwd()

# Procurar por arquivos .txt no diretório atual
for filename in os.listdir(current_directory):
    if filename.endswith('.txt'):
        substituir_espacos_por_hifen(filename)
# Buscas nas Securitizadoras
## VORTX
# ============================================================== FUNÇÃO ATUALIZADA ======================================

# Caminho para o WebDriver do Edge
driver_path = r"msedgedriver.exe"
edge_service = webdriver.edge.service.Service(driver_path)
driver = webdriver.Edge(service=edge_service)
driver.maximize_window()

def extrai_pu_vortx(url):

    print(f"Acessando o site: {url}")
    driver.get(url)
    

    try:
         # Navegando até a aba de PU
        wait = WebDriverWait(driver, 10) # Define a espera 
        aba_pu = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="radix-_r_0_-trigger-pu"]'))) # Espera o botão ser clicável
        aba_pu.click()
        # Navega até a última tabela
        # Espera até a lista de paginação estar presente
        pagination_ul = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'ul[data-slot="pagination-content"]'))
        )
        # Encontra todos os links de paginação
        pagination_links = pagination_ul.find_elements(By.CSS_SELECTOR, 'a[data-slot="pagination-link"][href]')
    except Exception as e:
        print(f"Erro ao buscar a tabela! \nExceção: {e}")
    max_page_num = -1
    max_page_element = None
    
    # Regex para extrair o número da página do href, ex: "...page=30"
    page_num_pattern = re.compile(r'page=(\d+)')

    try:
        # Itera até o link de paginação com o maior número -> leva a tabela mais recente
        for link in pagination_links:
            href = link.get_attribute('href')
            if href:
                match = page_num_pattern.search(href)
                if match:
                    page_num = int(match.group(1))
                    if page_num > max_page_num:
                        max_page_num = page_num
                        max_page_element = link
        
        if max_page_element:
            print(f"Clicando na última página: {max_page_num}")
            max_page_element.click()
        else:
            print("Nenhum link de página encontrado.")
            
    except Exception as e:
        print(f"Erro ao acessar navegar até o fim da tabela! \nExceção: {e}")
        return None

    try:
        # Espera até a tabela estar carregada 
        tabela = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'table'))
        )
        
        # Encontra todas as células de data
        celulas_data = tabela.find_elements(By.CSS_SELECTOR, 'td[data-slot="table-cell"]')
    except Exception as e:
        print(f"Erro ao interagir com a tabela! \nExceção: {e}")
        return None
    
    valor_extraido = None
    
    for celula in celulas_data:
        try:
            texto = celula.text.strip()
        except Exception as e:
            print(f"Erro ao interagir com a tabela! \nExceção: {e}")
            return None

        # Procura pela data atual
        if texto == data_atual_global:
            # Achou a célula da data, pega o elemento pai <tr>
            linha = celula.find_element(By.XPATH, './ancestor::tr')
            
            # Pega todas as células da linha
            celulas_linha = linha.find_elements(By.CSS_SELECTOR, 'td[data-slot="table-cell"]')
            
            # Navega até a 4ª coluna da linha (Coluna do PU)
            valor_celula = celulas_linha[3]  
            
            valor_extraido = valor_celula.text.strip()
            break
            
    # Testa a data anterior se não achar
    if valor_extraido is None:
        print("Buscando na data anterior")
        for celula in celulas_data:
            texto = celula.text.strip()
            # Procura pela data atual
            if texto == data_anterior_global:
                # Achou a célula da data, pega o elemento pai <tr>
                linha = celula.find_element(By.XPATH, './ancestor::tr')
                
                # Pega todas as células da linha
                celulas_linha = linha.find_elements(By.CSS_SELECTOR, 'td[data-slot="table-cell"]')
                
                # Navega até a 4ª coluna da linha (Coluna do PU)
                valor_celula = celulas_linha[3]  
                
                valor_extraido = valor_celula.text.strip()
                break
            
    if valor_extraido:
        print(f"Valor: {valor_extraido}")
        return valor_extraido
    else:
        print(f"Não achado na data atual nem anterior.")
        return None

# Ler o arquivo de texto
with open('pu_vortx.txt', 'r') as file:
    lines = file.readlines()

# Processar cada linha e atualizar as informações
updated_lines = []
for line in lines:
    line = line.strip()
    if not line or '-' not in line:
        continue
    
    # Dividir a linha em dois componentes: código do ativo e URL
    cod_ativo, site = line.split('-', 1)  # Dividir apenas na primeira ocorrência de hífen
    
    # Obter o valor de PU
    valor_pu = extrai_pu_vortx(site.strip())

    # Remove o separador de milhar
    if valor_pu is not None:
        valor_pu = valor_pu.replace(".","")
    else:
        print("Valor nulo... Pulando.")

    # Inserção do par na lista geral de Pu
    lista_pu_geral.append([cod_ativo, valor_pu])
    
    # Atualizar a linha com o valor encontrado
    if valor_pu:
        updated_line = f"{cod_ativo.strip()}-{valor_pu}-{site.strip()}\n"
    else:
        updated_line = f"{cod_ativo.strip()}--{site.strip()}\n"  # Manter a linha original se não achar o valor
    
    updated_lines.append(updated_line)

# Nome do arquivo de saída
output_file_name = 'pu_vortx_copia.txt'

# Escrever o arquivo atualizado
with open(output_file_name, 'w') as file:
    file.writelines(updated_lines)

# Criar pasta 'pu_excel' se não existir
output_directory = 'pu_excel'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Caminho completo para onde o arquivo será movido
destination_path = os.path.join(output_directory, output_file_name)

# Mover e substituir o arquivo
shutil.move(output_file_name, destination_path)

print(f"Processamento concluído. Arquivo '{output_file_name}' movido para a pasta '{output_directory}'.")
driver.quit()
## OPEA
# Caminho para o WebDriver do Edge
driver_path = r"msedgedriver.exe"
edge_service = webdriver.edge.service.Service(driver_path)
driver = webdriver.Edge(service=edge_service)
driver.maximize_window()

# Função para tentar capturar o XPath até 10 vezes, com tentativas adicionais
def capturar_xpath(xpath, tentativas=10, intervalo=1, tentativas_adicionais=5):
    for tentativa in range(tentativas):
        try:
            elemento = WebDriverWait(driver, intervalo).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            if elemento.text:  # Verifica se o elemento contém texto
                return elemento.text.strip()
        except (NoSuchElementException, TimeoutException):
            pass  # Se o elemento não for encontrado ou o tempo expirar, tenta novamente
        time.sleep(intervalo)  # Espera o intervalo entre tentativas

    # Se não encontrar na primeira tentativa, tenta mais 5 vezes
    for i in range(tentativas_adicionais):
        time.sleep(intervalo)  # Espera antes de tentar novamente
        try:
            elemento = WebDriverWait(driver, intervalo).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            if elemento.text:  # Verifica se o elemento contém texto
                return elemento.text.strip()
        except (NoSuchElementException, TimeoutException):
            continue  # Se não encontrar, continua tentando

    return "não localizado"  # Retorna "não localizado" após as tentativas

# Função para obter o valor de PU de um site
def obter_valor_pu(url):

    tentativa = 0
    
    try:
        
       while(tentativa < 2):

            print(f"Acessando o site: {url}")
            driver.get(url)
            print(f"tentativa: {tentativa}")
    
            if(tentativa == 0):
                # Usar apenas o primeiro XPath
                xpath = '//*[@id="app"]/div[2]/div/div[2]/div[1]/div[1]/div/div/div[1]/span'
                print(f"Tentando acessar: {xpath}")

            else:
                #Testa o segundo XPath
                xpath = '//*[@id="app"]/div[2]/div/div[2]/div[1]/div[1]/div/div/div[2]/span'
                print(f"Tentando acessar: {xpath}")
               
            # Tenta capturar o XPath
            valor = capturar_xpath(xpath)
            print(f"Conteúdo encontrado: {valor}")
    
            # Usar expressão regular para extrair o valor correto
            match = re.search(r'R\$ [\d,.]+', valor)    
            if match:
                break

            #Incrementa a tentativa
            tentativa = tentativa + 1
           
       if match:
            return match.group()
       else:
            print("Nenhum valor numérico encontrado no formato esperado.")
            return None

    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return None

# Ler o arquivo de texto
with open('pu_opea.txt', 'r') as file:
    lines = file.readlines()

# Processar cada linha e atualizar as informações
updated_lines = []
for line in lines:
    line = line.strip()
    if not line or '-' not in line:
        continue
    
    # Dividir a linha em dois componentes: código do ativo e URL
    cod_ativo, site = line.split('-', 1)  # Dividir apenas na primeira ocorrência de hífen
    
    # Obter o valor de PU
    valor_pu = obter_valor_pu(site.strip())
    
    # Remove o separador de milhar
    if valor_pu is not None:
        valor_pu = valor_pu.replace(".","")
    else:
        print("Valor nulo... Pulando.")

    # Inserção do par na lista geral de Pu
    lista_pu_geral.append([cod_ativo, valor_pu])
    
    # Atualizar a linha com o valor encontrado
    if valor_pu:
        updated_line = f"{cod_ativo.strip()}-{valor_pu}-{site.strip()}\n"
    else:
        updated_line = f"{cod_ativo.strip()}--{site.strip()}\n"  # Manter a linha original se não achar o valor
    
    updated_lines.append(updated_line)

# Nome do arquivo de saída
output_file_name = 'pu_opea_copia.txt'

# Escrever o arquivo atualizado
with open(output_file_name, 'w') as file:
    file.writelines(updated_lines)

# Criar pasta 'pu_excel' se não existir
output_directory = 'pu_excel'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Caminho completo para onde o arquivo será movido
destination_path = os.path.join(output_directory, output_file_name)

# Mover e substituir o arquivo
shutil.move(output_file_name, destination_path)

print(f"Processamento concluído. Arquivo '{output_file_name}' movido para a pasta '{output_directory}'.")
driver.quit()
## ECOAGRO
# Caminho para o WebDriver do Edge
driver_path = r"msedgedriver.exe"
edge_service = webdriver.edge.service.Service(driver_path)
driver = webdriver.Edge(service=edge_service)
driver.maximize_window()

# Função para tentar capturar o XPath até 10 vezes
def capturar_xpath(xpath, tentativas=10, intervalo=1):
    for tentativa in range(tentativas):
        try:
            elemento = driver.find_element(By.XPATH, xpath)
            if elemento.text:  # Verifica se o elemento contém texto
                return elemento.text.strip()
        except NoSuchElementException:
            pass  # Se o elemento não for encontrado, tenta novamente
        time.sleep(intervalo)  # Espera o intervalo entre tentativas
    return "não localizado"  # Retorna "não localizado" após as tentativas

# Função para obter o valor de PU de um site
def obter_valor_pu(url):
    try:
        print(f"Acessando o site: {url}")
        driver.get(url)

        # XPath base para as linhas da tabela
        xpath_base = "/html/body/main/section[2]/div/div[2]/table/tbody/tr[1]"

        # Data no formato esperado 
        # data_atual = datetime.now().strftime("%d.%m.%Y")

        # Data do dia anterior (para atualização do PU no horário da manhã)
        data_atual = data_anterior_raw
        data_atual = data_atual.strftime("%d.%m.%Y") 

        # Variável para armazenar o valor correspondente à data vigente
        valor_pu = None

        # Encontrar todas as linhas da tabela
        linhas = driver.find_elements(By.XPATH, xpath_base)

        # Iterar sobre as linhas da tabela
        for i, linha in enumerate(linhas):
            try:
                # XPath da data na linha atual
                xpath_data = f"{xpath_base}[{i + 1}]/td[1]"
                data = capturar_xpath(xpath_data)

                # Verificar se a data corresponde à data atual
                if data == data_atual:
                    # XPath do valor na linha atual
                    xpath_valor = f"{xpath_base}[{i + 1}]/td[2]"
                    valor_pu = capturar_xpath(xpath_valor)
                    break  # Sai do loop ao encontrar a data vigente
            except Exception as e:
                print(f"Erro ao processar linha {i + 1}: {e}")

        if valor_pu and valor_pu != "não localizado":
            print(f"Valor encontrado para a data {data_atual}: {valor_pu}")
            return valor_pu  # Retorna o valor exatamente como foi encontrado
        else:
            print(f"Nenhum valor encontrado para a data {data_atual}.")
            return None

    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return None

# Ler o arquivo de texto
with open('pu_ecoagro.txt', 'r') as file:
    lines = file.readlines()

# Processar cada linha e atualizar as informações
updated_lines = []
for line in lines:
    line = line.strip()
    if not line or '-' not in line:
        continue
    
    # Dividir a linha em dois componentes: código do ativo e URL
    cod_ativo, site = line.split('-', 1)  # Dividir apenas na primeira ocorrência de hífen
    
    # Obter o valor de PU
    valor_pu = obter_valor_pu(site.strip())

    # Inserção do par na lista geral de Pu
    lista_pu_geral.append([cod_ativo, valor_pu])
    
    # Atualizar a linha com o valor encontrado
    if valor_pu:
        updated_line = f"{cod_ativo.strip()}-{valor_pu}-{site.strip()}\n"
    else:
        updated_line = f"{cod_ativo.strip()}--{site.strip()}\n"  # Manter a linha original se não achar o valor
    
    updated_lines.append(updated_line)

# Nome do arquivo de saída
output_file_name = 'pu_ecoagro_copia.txt'

# Escrever o arquivo atualizado
with open(output_file_name, 'w') as file:
    file.writelines(updated_lines)

# Criar pasta 'pu_excel' se não existir
output_directory = 'pu_excel'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Caminho completo para onde o arquivo será movido
destination_path = os.path.join(output_directory, output_file_name)

# Mover e substituir o arquivo
shutil.move(output_file_name, destination_path)

print(f"Processamento concluído. Arquivo '{output_file_name}' movido para a pasta '{output_directory}'.")
driver.quit()
## OLIVEIRA-TRUST
# Caminho para o WebDriver do Edge
driver_path = r"msedgedriver.exe"
edge_service = webdriver.edge.service.Service(driver_path)
driver = webdriver.Edge(service=edge_service)
driver.maximize_window()

# Função para tentar capturar o XPath até 10 vezes, procurando um número
def capturar_xpath(xpath, tentativas=10, intervalo=1):
    for tentativa in range(tentativas):
        try:
            elemento = driver.find_element(By.XPATH, xpath)
            texto = elemento.text.strip()
            # Verifica se o texto contém um número no formato desejado
            match = re.search(r'R\$ [\d,.]+', texto)
            if match:
                return match.group()  # Retorna o número encontrado
        except NoSuchElementException:
            pass  # Se o elemento não for encontrado, tenta novamente
        time.sleep(intervalo)  # Espera o intervalo entre tentativas
    return "não localizado"  # Retorna "não localizado" após as tentativas

# Função para obter o valor de PU de um site
def obter_valor_pu(url):
    try:
        print(f"Acessando o site: {url}")
        driver.get(url)

        # Usar apenas o primeiro XPath
        xpath = '//*[@id="__nuxt"]/div/main/div/div/div[2]/div/div/div/div[4]/span'
        print(f"Tentando acessar: {xpath}")

        # Tenta capturar o XPath até 10 vezes
        valor = capturar_xpath(xpath)
        print(f"Conteúdo encontrado: {valor}")

        # Retorna o valor exatamente como foi encontrado
        if valor and valor != "não localizado":
            return valor
        else:
            print("Nenhum valor numérico encontrado.")
            return None

    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return None

# Ler o arquivo de texto
with open('pu_oliveiratrust.txt', 'r') as file:
    lines = file.readlines()

# Processar cada linha e atualizar as informações
updated_lines = []
for line in lines:
    line = line.strip()
    if not line or '-' not in line:
        continue
    
    # Dividir a linha em dois componentes: código do ativo e URL
    cod_ativo, site = line.split('-', 1)  # Dividir apenas na primeira ocorrência de hífen
    
    # Obter o valor de PU
    valor_pu = obter_valor_pu(site.strip())

    # Remove o separador de milhar
    if valor_pu is not None:
        valor_pu = valor_pu.replace(".","")
    else:
        print("Valor nulo... Pulando.")
    
    # Inserção do par na lista geral de Pu
    lista_pu_geral.append([cod_ativo, valor_pu])
    
    # Atualizar a linha com o valor encontrado
    if valor_pu:
        updated_line = f"{cod_ativo.strip()}-{valor_pu}-{site.strip()}\n"
    else:
        updated_line = f"{cod_ativo.strip()}--{site.strip()}\n"  # Manter a linha original se não achar o valor
    
    updated_lines.append(updated_line)

# Nome do arquivo de saída
output_file_name = 'pu_oliveiratrust_copia.txt'

# Escrever o arquivo atualizado
with open(output_file_name, 'w') as file:
    file.writelines(updated_lines)

# Criar pasta 'pu_excel' se não existir
output_directory = 'pu_excel'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Caminho completo para onde o arquivo será movido
destination_path = os.path.join(output_directory, output_file_name)

# Mover e substituir o arquivo
shutil.move(output_file_name, destination_path)

print(f"Processamento concluído. Arquivo '{output_file_name}' movido para a pasta '{output_directory}'.")
driver.quit()
## FIDUCIARIO
# Caminho para o WebDriver do Edge
driver_path = r"msedgedriver.exe"
edge_service = webdriver.edge.service.Service(driver_path)
driver = webdriver.Edge(service=edge_service)
driver.maximize_window()

# Função para tentar capturar o XPath até 10 vezes
def capturar_xpath(xpath, tentativas=10, intervalo=1):
    for tentativa in range(tentativas):
        try:
            elemento = driver.find_element(By.XPATH, xpath)
            if elemento.text:  # Verifica se o elemento contém texto
                return elemento.text.strip()
        except NoSuchElementException:
            pass  # Se o elemento não for encontrado, tenta novamente
        time.sleep(intervalo)  # Espera o intervalo entre tentativas
    return "não localizado"  # Retorna "não localizado" após as tentativas

# Função para obter o valor de PU de um site
def obter_valor_pu(url):
    try:
        print(f"Acessando o site: {url}")
        driver.get(url)

        # Usar apenas o primeiro XPath
        xpath = '/html/body/section[1]/div/div/div[2]/div/div[3]/div[1]/div/div[1]/span[2]'
        print(f"Tentando acessar: {xpath}")

        # Tenta capturar o XPath até 10 vezes
        valor = capturar_xpath(xpath)
        print(f"Conteúdo encontrado: {valor}")

        # Usar expressão regular para extrair o valor correto
        match = re.search(r'R\$ [\d,.]+', valor)
        if match:
            return match.group()
        else:
            print("Nenhum valor numérico encontrado no formato esperado.")
            return None

    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return None

# Ler o arquivo de texto
with open('pu_fiduciario.txt', 'r') as file:
    lines = file.readlines()

# Processar cada linha e atualizar as informações
updated_lines = []
for line in lines:
    line = line.strip()
    if not line or '-' not in line:
        continue
    
    # Dividir a linha em dois componentes: código do ativo e URL
    cod_ativo, site = line.split('-', 1)  # Dividir apenas na primeira ocorrência de hífen
    
    # Obter o valor de PU
    valor_pu = obter_valor_pu(site.strip())

    # Inserção do par na lista geral de Pu
    lista_pu_geral.append([cod_ativo, valor_pu])
    
    # Atualizar a linha com o valor encontrado
    if valor_pu:
        updated_line = f"{cod_ativo.strip()}-{valor_pu}-{site.strip()}\n"
    else:
        updated_line = f"{cod_ativo.strip()}--{site.strip()}\n"  # Manter a linha original se não achar o valor
    
    updated_lines.append(updated_line)

# Nome do arquivo de saída
output_file_name = 'pu_fiduciario_copia.txt'

# Escrever o arquivo atualizado
with open(output_file_name, 'w') as file:
    file.writelines(updated_lines)

# Criar pasta 'pu_excel' se não existir
output_directory = 'pu_excel'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Caminho completo para onde o arquivo será movido
destination_path = os.path.join(output_directory, output_file_name)

# Mover e substituir o arquivo
shutil.move(output_file_name, destination_path)

print(f"Processamento concluído. Arquivo '{output_file_name}' movido para a pasta '{output_directory}'.")
driver.quit()
# Pentágono
# # ========================================================= VERSÃO ANTIGA ============================================


# # Caminho para o WebDriver do Edge
# driver_path = r"msedgedriver.exe"
# edge_service = webdriver.edge.service.Service(driver_path)
# driver = webdriver.Edge(service=edge_service)
# driver.maximize_window()

# # Função para tentar capturar o XPath até 10 vezes
# def capturar_xpath(xpath, tentativas=10, intervalo=1):
#     for tentativa in range(tentativas):
#         try:
#             elemento = driver.find_element(By.XPATH, xpath)
#             if elemento.text:  # Verifica se o elemento contém texto
#                 return elemento.text.strip()
#         except NoSuchElementException:
#             pass  # Se o elemento não for encontrado, tenta novamente
#         time.sleep(intervalo)  # Espera o intervalo entre tentativas
#     return "não localizado"  # Retorna "não localizado" após as tentativas

# # Função para obter o valor de PU de um site
# def obter_valor_pu(url):
    
#     try:
#         print(f"Acessando o site: {url}")
#         driver.get(url)

#         # Extrai o Xpath com base no tamanho da tabela presente (O tamanho da tabela pode variar entre ativos)
#         numberSpaces =  driver.find_element(By.XPATH, '//*[@id="tab-4"]/div/div[4]/div[1]/table/thead/tr').text.count(' ')# Se numberSpaces=9->6colunas
#                                                                                                                     # Se numberSpaces = 14 -> 8 colunas
#         if(numberSpaces == 9):
#             xpath = '//*[@id="tab-4"]/div/div[4]/div[1]/table/tbody/tr[1]/td[5]'
#             # Verifica se a data corresponde ao dia de hoje:
#             todayDate = driver.find_element(By.XPATH, '//*[@id="tab-4"]/div/div[4]/div[1]/table/tbody/tr[1]/td[1]').text
#             print("PU Referente à Data: ", todayDate)
#         else:
#             xpath = '//*[@id="tab-4"]/div/div[4]/div[1]/table/tbody/tr[1]/td[7]'
#             # Verifica se a data corresponde ao dia de hoje:
#             todayDate = driver.find_element(By.XPATH, '//*[@id="tab-4"]/div/div[4]/div[1]/table/tbody/tr[1]/td[1]').text
#             print("PU Referente à Data: ", todayDate)
            
#         # Tenta capturar o XPath até 10 vezes
#         valor = capturar_xpath(xpath)
#         print(f"Conteúdo encontrado: {valor}")

#         return valor

#     except Exception as e:
#         print(f"Erro ao acessar {url}: {e}")
#         return None

# # Ler o arquivo de texto
# with open('pu_pentagono.txt', 'r') as file:
#     lines = file.readlines()

# # Processar cada linha e atualizar as informações
# updated_lines = []
# for line in lines:
#     line = line.strip()
#     if not line or '-' not in line:
#         continue
    
#     # Dividir a linha em dois componentes: código do ativo e URL
#     cod_ativo, site = line.split('-', 1)  # Dividir apenas na primeira ocorrência de hífen
    
#     # Obter o valor de PU
#     valor_pu = obter_valor_pu(site.strip())

#     # Remove o separador de milhar
#     if valor_pu is not None:
#         valor_pu = valor_pu.replace(".","")
#     else:
#         print("Valor nulo... Pulando.")

#     # Inserção do par na lista geral de Pu
#     lista_pu_geral.append([cod_ativo, valor_pu])
    
#     # Atualizar a linha com o valor encontrado
#     if valor_pu:
#         updated_line = f"{cod_ativo.strip()}-{valor_pu}-{site.strip()}\n"
#     else:
#         updated_line = f"{cod_ativo.strip()}--{site.strip()}\n"  # Manter a linha original se não achar o valor
    
#     updated_lines.append(updated_line)

# # Nome do arquivo de saída
# output_file_name = 'pu_pentagono_copia.txt'

# # Escrever o arquivo atualizado
# with open(output_file_name, 'w') as file:
#     file.writelines(updated_lines)

# # Criar pasta 'pu_excel' se não existir
# output_directory = 'pu_excel'
# if not os.path.exists(output_directory):
#     os.makedirs(output_directory)

# # Caminho completo para onde o arquivo será movido
# destination_path = os.path.join(output_directory, output_file_name)

# # Mover e substituir o arquivo
# shutil.move(output_file_name, destination_path)

# print(f"Processamento concluído. Arquivo '{output_file_name}' movido para a pasta '{output_directory}'.")
# driver.quit()
# ========================================================= VERSÃO ATUALIZADA ============================================


# Caminho para o WebDriver do Edge
driver_path = r"msedgedriver.exe"
edge_service = webdriver.edge.service.Service(driver_path)
driver = webdriver.Edge(service=edge_service)
driver.maximize_window()

# Função para tentar capturar o XPath até 10 vezes
def capturar_xpath(xpath, tentativas=10, intervalo=1):
    for tentativa in range(tentativas):
        try:
            elemento = driver.find_element(By.XPATH, xpath)
            if elemento.text:  # Verifica se o elemento contém texto
                return elemento.text.strip()
        except NoSuchElementException:
            pass  # Se o elemento não for encontrado, tenta novamente
        time.sleep(intervalo)  # Espera o intervalo entre tentativas
    return "não localizado"  # Retorna "não localizado" após as tentativas

# Função para obter o valor de PU de um site
def obter_valor_pu(url):
    
    try:
        print(f"Acessando o site: {url}")
        driver.get(url)

        # Espera a tabela carregar (opcional: ajustar com WebDriverWait)
        tabela = driver.find_element(By.XPATH, '//*[@id="tab-4"]/div/div[4]/div[1]/table')

        # Pega os nomes das colunas no cabeçalho
        cabecalhos = tabela.find_elements(By.XPATH, ".//thead/tr/th")
        nomes_colunas = [th.text.strip().upper() for th in cabecalhos]

        # Extrai os índices
        idx_data = nomes_colunas.index("DATA") + 1
        idx_pu = nomes_colunas.index("PU") + 1

        # Extrai todas as linhas da tabela
        linhas = tabela.find_elements(By.XPATH, ".//tbody/tr")

        # Encontra a linha com a data de hoje
        pu_hoje = None
        for linha in linhas:
            data_texto = linha.find_element(By.XPATH, f"./td[{idx_data}]").text.strip()
            if data_texto == data_atual_global:
                pu_hoje = linha.find_element(By.XPATH, f"./td[{idx_pu}]").text.strip()
                print(f"PU referente à data de hoje ({data_atual_global}): {pu_hoje}")
                break

        if pu_hoje is None:
            print(f"Nenhuma linha encontrada para a data de hoje ({data_atual_global}).")
        return pu_hoje
            
    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return None

# Ler o arquivo de texto
with open('pu_pentagono.txt', 'r') as file:
    lines = file.readlines()

# Processar cada linha e atualizar as informações
updated_lines = []
for line in lines:
    line = line.strip()
    if not line or '-' not in line:
        continue
    
    # Dividir a linha em dois componentes: código do ativo e URL
    cod_ativo, site = line.split('-', 1)  # Dividir apenas na primeira ocorrência de hífen
    
    # Obter o valor de PU
    valor_pu = obter_valor_pu(site.strip())

    # Remove o separador de milhar
    if valor_pu is not None:
        valor_pu = valor_pu.replace(".","")
    else:
        print("Valor nulo... Pulando.")

    # Inserção do par na lista geral de Pu
    lista_pu_geral.append([cod_ativo, valor_pu])
    
    # Atualizar a linha com o valor encontrado
    if valor_pu:
        updated_line = f"{cod_ativo.strip()}-{valor_pu}-{site.strip()}\n"
    else:
        updated_line = f"{cod_ativo.strip()}--{site.strip()}\n"  # Manter a linha original se não achar o valor
    
    updated_lines.append(updated_line)

# Nome do arquivo de saída
output_file_name = 'pu_pentagono_copia.txt'

# Escrever o arquivo atualizado
with open(output_file_name, 'w') as file:
    file.writelines(updated_lines)

# Criar pasta 'pu_excel' se não existir
output_directory = 'pu_excel'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Caminho completo para onde o arquivo será movido
destination_path = os.path.join(output_directory, output_file_name)

# Mover e substituir o arquivo
shutil.move(output_file_name, destination_path)

print(f"Processamento concluído. Arquivo '{output_file_name}' movido para a pasta '{output_directory}'.")
driver.quit()
# Vert
# Caminho para o WebDriver do Edge
driver_path = r"msedgedriver.exe"
edge_service = webdriver.edge.service.Service(driver_path)
driver = webdriver.Edge(service=edge_service)
driver.maximize_window()

# Função para tentar capturar o XPath até 10 vezes, procurando um número
def capturar_xpath(xpath, tentativas=10, intervalo=1):
    for tentativa in range(tentativas):
        try:
            # Aguarda o carregamento do label com texto "P.U. atualizado"
            espera = WebDriverWait(driver, 10)
            
            # Primeiro encontra o label "P.U. atualizado"
            label_pu = espera.until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'P.U. atualizado')]")))
            
            # Depois encontra o próximo label (irmão seguinte)
            label_valor = espera.until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'P.U. atualizado')]/following-sibling::label[1]"))).text
            
            # Verifica se o texto contém um número no formato desejado
            match = re.search(r'[\d,.]+', label_valor)
            if match:
                return match.group()  # Retorna o número encontrado
        except NoSuchElementException:
            pass  # Se o elemento não for encontrado, tenta novamente
        time.sleep(intervalo)  # Espera o intervalo entre tentativas
    return "não localizado"  # Retorna "não localizado" após as tentativas

# Função para obter o valor de PU de um site
def obter_valor_pu(url):
    try:
        print(f"Acessando o site: {url}")
        driver.get(url)

        # Usar apenas o primeiro XPath
        xpath = '/html/body/div/div/div/div[3]/div/div/div[2]/div/div[3]/div[1]/div/div/div[1]/div[6]/label[2]' 
        print(f"Tentando acessar: {xpath}")

        # Tenta capturar o XPath até 10 vezes
        valor = capturar_xpath(xpath)
        print(f"Conteúdo encontrado: {valor}")

        # Retorna o valor exatamente como foi encontrado
        if valor and valor != "não localizado":
            return valor
        else:
            print("Nenhum valor numérico encontrado.")
            return None

    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return None

# Ler o arquivo de texto
with open('pu_vert.txt', 'r') as file:
    lines = file.readlines()

# Processar cada linha e atualizar as informações
updated_lines = []
for line in lines:
    line = line.strip()
    if not line or '-' not in line:
        continue
    
    # Dividir a linha em dois componentes: código do ativo e URL
    cod_ativo, site = line.split('-', 1)  # Dividir apenas na primeira ocorrência de hífen
    
    # Obter o valor de PU
    valor_pu = obter_valor_pu(site.strip())

    # Troca o marcador de decimal
    if valor_pu is not None:
        valor_pu = valor_pu.replace(".",",")
    else:
        print("Valor nulo... Pulando.")

    # Inserção do par na lista geral de Pu
    lista_pu_geral.append([cod_ativo, valor_pu])
    
    # Atualizar a linha com o valor encontrado
    if valor_pu:
        updated_line = f"{cod_ativo.strip()}-{valor_pu}-{site.strip()}\n"
    else:
        updated_line = f"{cod_ativo.strip()}--{site.strip()}\n"  # Manter a linha original se não achar o valor
    
    updated_lines.append(updated_line)

# Nome do arquivo de saída
output_file_name = 'pu_vert_copia.txt'

# Escrever o arquivo atualizado
with open(output_file_name, 'w') as file:
    file.writelines(updated_lines)

# Criar pasta 'pu_excel' se não existir
output_directory = 'pu_excel'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Caminho completo para onde o arquivo será movido
destination_path = os.path.join(output_directory, output_file_name)

# Mover e substituir o arquivo
shutil.move(output_file_name, destination_path)

print(f"Processamento concluído. Arquivo '{output_file_name}' movido para a pasta '{output_directory}'.")
driver.quit()
# CRA's Dólar

* CRA022008N5
* CRA024004S9

Sites para consulta:
>https://www.selenium.dev/documentation/webdriver/elements/finders/

>https://www.selenium.dev/documentation/test_practices/encouraged/locators/
def extrai_dolar():
    '''
    Extrai o valor do dólar para o dia anterior a partir do site referenciado pelo banco central. 
    '''

    # Configuração do driver
    driver_path = r"msedgedriver.exe"
    edge_service = webdriver.edge.service.Service(driver_path)
    driver = webdriver.Edge(service=edge_service)
    driver.maximize_window()
    
    # Formata a data para conterem apenas números
    data_atual_plain = data_atual_global.replace("/","")
    data_anterior_plain = data_anterior_raw.strftime("%d/%m/%Y").replace("/","")
    
    
    # Acesso ao site
    driver.get("https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=consultarBoletim")
    
    # Clica no botão para selecionar a opção de pesquisa
    driver.find_element(By.ID, "RadOpcao").click()
    
    # Insere a data inicial e final
    driver.find_element(By.ID, "DATAINI").clear()
    driver.find_element(By.ID, "DATAINI").send_keys(data_anterior_plain)
    
    driver.find_element(By.ID, "DATAFIM").clear()
    driver.find_element(By.ID, "DATAFIM").send_keys(data_atual_plain)
    
    # Muda a moeda para dólar americano no menu suspenso -> Dolar = Value 61
    dropdown_menu = driver.find_element(By.XPATH, "/html/body/div[4]/form/table[2]/tbody/tr[4]/td[2]/select") # Xpath do menu suspenso
    
    # Cria um objeto de Seletor e seleciona o valor do dólar americano
    select_dropdown = Select(dropdown_menu)
    select_dropdown.select_by_value("61")
    
    # Pesquisa
    wait = WebDriverWait(driver, 10) # Define a espera 
    pesquisar = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and @value='Pesquisar']"))) # Espera o botão ser clicável
    pesquisar.click()
    
    # Espera a nova aba carregar
    time.sleep(1)
    
    # Extrai o valor a partir da tabela exibida
    dolar_hoje = driver.find_element(By.XPATH, "/html/body/div/table/tbody/tr[3]/td[3]").text
    
    # Sai do Driver
    driver.quit()

    return dolar_hoje
def extrai_pu_dolar(cod_ativo, linha_header, col_data, col_pu, col_dolar, valor_dolar):

    """
    Extrai o PU para um ativo precificado com base no dólar. Nota-se que já existem fórmulas pré configuradas na planilha de excel consumida.
    Com a inserção do novo valor do dólar, o PU é resolvido automaticamente a partir destas fórmulas, não havendo necessidade de calculá-lo aqui.
    """

    # Converte os argumentos da função para números a fim de indexar a planilha
    linha_header = int(linha_header)
    col_data = int(col_data)
    col_pu = int(col_pu)
    col_dolar = int(col_dolar)

    # Compõe o nome do arquivo para o ativo:
    ativo_file = path_dir + "\\" + cod_ativo + ".xlsx"
    
    # Abertura do arquivo:
    df_ativo = pd.read_excel(ativo_file, header=linha_header)

    # Remoção das linhas inuteis do cabeçalho
    df_ativo.drop(index=[11,12,13,14], axis=0, inplace=True)

    # Resetando o índice
    df_ativo.reset_index()
    
    # Formatação da data
    df_ativo[0] = pd.to_datetime(df_ativo[df_ativo.columns[col_data]], dayfirst=True, errors='coerce')

    # Data atual no formato esperado
    data_atual = pd.to_datetime(datetime.now().date())

    # Extração do índice referente ao dia atual
    indice_hoje = np.where(df_ativo[col_data] == data_atual)[0]

    # Inserção do valor do dólar no dia atual
    df_ativo.iloc[indice_hoje, col_dolar] = valor_dolar

    # Captura do PU calculado
    pu_dolar_hoje = df_ativo.iloc[indice_hoje, col_pu]

    # =================== DEBUG ========
    print("PU Dolar Interno: ", pu_dolar_hoje)
    #print("",)
    #print("",)
    
    return pu_dolar_hoje
## =============================================== NÃO FUNCIONANDO - Refatorar com openpyxl ================================================

# def pu_dolar(pu_diario_file):

#     """
#     Extrai os PUs dos ativos especificados na aba "CRA_Dolar" da planilha de PU diário.
#     """
    
#     # Abre a planilha
#     df_source = pd.read_excel(pu_diario_file, 
#                               sheet_name="CRA_Dolar")

#     print(df_source)

#     # Extrai a cotação do dólar para o dia anterior
#     dolar = extrai_dolar()
#     print("Dólar para o dia anterior: R$ ", dolar)

#     #Itera sobre o Dataframe, extraindo cada PU
#     for i in range(len(df_source)):

#         # Abre a planilha do ativo para esta iteração
#         print("Calculando o PU para o ativo:", df_source.iloc[i,0])
#         work_pu_dolar = extrai_pu_dolar(df_source.iloc[i,0],
#                                             df_source.iloc[i,1],
#                                             df_source.iloc[i,2], 
#                                             df_source.iloc[i,3],
#                                             df_source.iloc[i,4],
#                                             dolar)

#         # Esta retornando como série, transformado em lista 
#         work_pu_dolar = work_pu_dolar.tolist()
#         work_pu_dolar = work_pu_dolar[0]
        
#         print("Ativo: ", df_source.iloc[i,0], " | PU: ", work_pu_dolar)

#         # Extrai o PU e guarda na lista pré instanciada
#         lista_pu_geral.append([df_source.iloc[i,0], work_pu_dolar])
#pu_dolar(pu_diario_file)
# Diretório

Esta parte do script será responsável por extrair o PU dos ativos a partir das planilhas **EM FORMATO `.XLSX`** presentes no diretório. Para tanto, devido às diferenças na formatação de cada planilha, é necessário informar a linha em que estão os cabeçalhos, a coluna em que estão os PU's e as Datas. Também é necessário que a aba que contenha o PU esteja em primeiro na planilha de cada ativo.

* Criar uma função que abra as planilhas dos ativos a terem seus PUs extraidos do diretório.

* Criar uma função que receba os parametros: Dataframe da planilha de PU, linha do cabeçalho, coluna da data e coluna do PU e abra o arquivo, padronizando as colunas em tipo de dado e nome.

* Criar uma função que receba o dataframe formatado e extraia o PU da data atual
def abre_planilha_ativo(cod_ativo, linha_header, col_data, col_pu):

    '''
    Abre a planilha de um ativo adequadamente, deixando explicito a coluna de data e PU. Retorna um DF com as colunas Data e Pu
    '''

    # Converte os argumentos da função para números a fim de indexar a planilha
    linha_header = int(linha_header)
    col_data = int(col_data)
    col_pu = int(col_pu)
    
    # Compõe o nome do arquivo para o ativo:
    ativo_file = path_dir + "\\" + cod_ativo + ".xlsx"

    # Abre o Arquivo com os parametros especificados
    df_ativo = pd.read_excel(ativo_file, 
                             header = linha_header,
                             usecols = [col_data, col_pu],
                             parse_dates=[0])

    # print("ANTES DE FORMATAR!!!\n")
    # print(df_ativo.head())

    # Padroniza o nome das colunas
    df_ativo.columns = ["Data","PU"]
    df_ativo["Data"] = pd.to_datetime(df_ativo["Data"], dayfirst=True, errors='coerce')
    df_ativo["Data"] = df_ativo["Data"].dt.strftime("%d/%m/%Y")
    

    # print("DEPOIS DE FORMATAR!!!\n")
    # print(df_ativo.head())
    # print(df_ativo.shape)

    return df_ativo
def extrai_pu_diario(df_ativo):
    
    """
    Extrai o PU do ativo para o dia atual a partir de um dataframe correspondente à planilha de PU para um ativo.
    """

    # #Definição da data atual, no formato esperado
    # data_atual = pd.to_datetime(datetime.now().date())
    # data_atual_except = datetime.now().strftime("%Y-%m-%d")

    try:
        # Extrai o indice correspondente ao dia de hoje com a data no form
        indice_hoje = np.where(df_ativo['Data'] == data_atual_global)[0]
        print("Indice Hoje:", indice_hoje)

    except:
        # Extrai o indice correspondente ao dia de hoje com a data no form
        indice_hoje = np.where(df_ativo['Data'] == data_atual_except)[0]
        print("Indice Hoje Except:", indice_hoje)
        

    try:
       # Extrai o PU do dia de hoje a partir do Dataframe passada para a função
        pu_hoje = df_ativo.iloc[indice_hoje[0], 1]
        print("Pu para o dia ", data_atual_global, ": ", pu_hoje)

    except:
        # Caso surja um erro, joga o PU para 0
        pu_hoje = "0,0"
        print("Erro na extração do PU! Verificar a planilha no diretório")

    # Remove o separador de milhar
    if pu_hoje is not None:
        if type(pu_hoje) is str:
            pu_hoje = pu_hoje.replace(".",",")
        else:
            pu_hoje = pu_hoje.astype(str).replace(".",",")
    else:
        print("Valor nulo... Pulando.")

    return pu_hoje
### Célula para teste de ativos novos

Descomentar apenas para testes
# Abre o Arquivo com os parametros especificados
ativo_file = r"C:\Users\bruno.loffreda\C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A\Back Investimentos - Documentos\Transferencia custodia\Atualização PU\VIRGOPENTAGONO\12F0036335.xlsx"


df_ativo = pd.read_excel(ativo_file, 
                            header = 0,
                            usecols = [6, 60],
                            parse_dates=[0])
                    
#Padroniza o nome das colunas
df_ativo.columns = ["Data","PU"]
df_ativo["Data"] = pd.to_datetime(df_ativo["Data"], dayfirst=True, errors='coerce')
print(df_ativo.head())
print(df_ativo.dtypes)
# Extrai o indice correspondente ao dia de hoje com a data no form
indice_hoje = np.where(df_ativo['Data'] == data_atual_global)[0]
print("Indice Hoje:", indice_hoje)

pu_hoje = df_ativo.iloc[indice_hoje[0], 1]
print("Pu para o dia ", data_atual_global, ": ", pu_hoje)

# # Teste com as funções
# df_ativo = abre_planilha_ativo("12F0036335", 0, 6, 60)
# extrai_pu_diario(df_ativo)

def pu_diretorio(pu_diario_file):

    """
    Extrai os PUs doas ativos especificados na aba "Diretorio" da planilha de PU diário.
    """
    
    # Abre a planilha
    df_source = pd.read_excel(pu_diario_file, 
                              sheet_name="Diretorio")

    print(df_source)

    #Itera sobre o Dataframe, extraindo cada PU
    for i in range(len(df_source)):

        # Abre a planilha do ativo para esta iteração
        print("Tentando abrir planilha do ativo:", df_source.iloc[i,0])
        work_df_ativo = abre_planilha_ativo(df_source.iloc[i,0],
                                            df_source.iloc[i,1],
                                            df_source.iloc[i,2], 
                                            df_source.iloc[i,3])

        print("Planilha lida com sucesso!")

        # Extração do PU
        work_pu = extrai_pu_diario(work_df_ativo)
        print("PU Extraído com sucesso!")

        # Extrai o PU e guarda na lista pré instanciada
        lista_pu_geral.append([df_source.iloc[i,0], work_pu])
# Arquivos Cetip

Estas funções calculam o PU diários para os ativos precificados através dos arquivos baixados da CETIP (LIG's e algumas debêntures).

* Processar os arquivos baixados da CETIP, criando uma nova coluna pa o PU como (valor atual da emissão + juros)

* Ingerir o arquivo dos PU's e identificar quais ativos são precificados a partir de qual arquivo.

* Extrair o PU para cada ativo identificado
def cria_pu_cetip(pu_diario_file):

    """
    Recebe a planilha de PU diário e extrai os PU's dos ativos precificados pela CETIP. Parte do pressuposto que o arquivo extraído da cetip ja foi 
    convertido em .csv e se encontra na pasta downloads do usuário.
    """

    # Formatação da data
    data_anterior_cetip = data_anterior_raw.strftime("%y%m%d")
    
    # Definição do caminho especifico para cada arquivo 
    path_imob = path_downloads + "\\29590_" + data_anterior_cetip  + "_DCUSTODIAPART-IMOB.csv"
    path_deb = path_downloads + "\\29590_" + data_anterior_cetip  + "_DCUSTODIAPART-DEB.csv"

    # Formatando os arquivos
    df_imob_format = formata_arquivo_imob(path_imob)
    df_deb_format = formata_arquivo_deb(path_deb)

    # Identificando os ativos em cada caso a partir do arquivo do PU Diário
    df_imob, df_deb = identifica_ativos(pu_diario_file)

    # Extraindo o PU para cada ativo
    lista_imob = extrai_pu_cetip(df_imob_format, df_imob)
    lista_deb = extrai_pu_cetip(df_deb_format, df_deb)

    # Insere os pares [Ativo, PU] identificados na lista geral
    lista_pu_geral.extend(lista_imob)
    lista_pu_geral.extend(lista_deb)

    print("Arquivos CETIP processados!")
def identifica_ativos(pu_diario_file):

    """
    Abre a planilha do PU diário a fim de identificar quais ativos são precificados pela CETIP. Retorna um dataframe diferente para cada fonte
    que depois será usado para procurar o PU no arquivo correto da CETIP.
    """

    # Abre a planilha
    df_source = pd.read_excel(pu_diario_file,
                         usecols=[1,3])

    # Identificando as LIG's
    df_ativos_imob = df_source[df_source["Fonte"] == "CETIP IMOB"]
    
    # Identificando as DEB's
    df_ativos_deb = df_source[df_source["Fonte"] == "CETIP DEB"]

    return  df_ativos_imob, df_ativos_deb
def formata_arquivo_imob(path_imob):

    '''
    Formata o arquivo referente às LIGs, recebendo o caminho do arquivo
    '''
    
    # Abrindo o arquivo apenas com as colunas necessárias
    df_imob = pd.read_csv(path_imob, 
                      delimiter = ";",
                      usecols=[4, 6, 18])

    # Normalizando os valores (trocando o separador decimal)
    df_imob["Valor de Emissao Atual"] = df_imob["Valor de Emissao Atual"].str.replace(",", '.', regex=True).fillna('0').astype(float)
    df_imob["Juros Acumulados"] = df_imob["Juros Acumulados"].str.replace(",", '.', regex=True).fillna('0').astype(float)


    # Criando a coluna para o PU, somando Valor de Emissao Atual e Juros Acumulados
    df_imob["PU"] = (df_imob["Valor de Emissao Atual"] + df_imob["Juros Acumulados"])
    df_imob["PU"] = df_imob["PU"].apply(lambda x: '%.8f' % x)

    # Reconvertendo o valor para string a fim de trocar o separador decimal
    df_imob["PU"] = df_imob["PU"].astype(str)
    df_imob["PU"] = df_imob["PU"].str.replace(".", ',')

    return df_imob
def formata_arquivo_deb(path_deb):

    '''
    Formata o arquivo referente às Debêntures, recebendo o caminho do arquivo
    '''
    
    # Abrindo o arquivo apenas com as colunas necessárias
    df_deb = pd.read_csv(path_deb, 
                      delimiter = ";",
                      usecols=[3,5,17],
                      encoding='latin-1')

    # Normalizando os valores (trocando o separador decimal)
    df_deb["Valor de Emissao Atual"] = df_deb["Valor de Emissao Atual"].str.replace(",", '.', regex=True).fillna('0').astype(float)
    df_deb["Juros Acumulados"] = df_deb["Juros Acumulados"].str.replace(",", '.', regex=True).fillna('0').astype(float)


    # Criando a coluna para o PU, somando Valor de Emissao Atual e Juros Acumulados
    df_deb["PU"] = (df_deb["Valor de Emissao Atual"] + df_deb["Juros Acumulados"])
    df_deb["PU"] = df_deb["PU"].apply(lambda x: '%.8f' % x)

    # Reconvertendo o valor para string a fim de trocar o separador decimal
    df_deb["PU"] = df_deb["PU"].astype(str)
    df_deb["PU"] = df_deb["PU"].str.replace(".", ',')

    return df_deb
def extrai_pu_cetip(df_fonte_format, df_ativos):

    """
    Recebe o arquivo fonte da cetip formatado a lista de ativos a ser pesquisada e extrai o PU. Retorna uma lista contendo os pares [Ativo, PU]
    """
    # Faz a junção das planilhas com base no Código de Ativo
    df_result = pd.merge(df_ativos, df_fonte_format, left_on="Cod.Ativo", right_on="Ativo")
    
    # Dropa as colunas não utilizadas resultantes do join
    df_result.drop(labels=["Cod.Ativo", "Fonte", "Valor de Emissao Atual", "Juros Acumulados"], inplace=True, axis=1)

    # Instancia a lista a ser devolvida
    lista_pu = []
    # Itera sobre o Dataframe a fim de devolver uma lista
    for i in range(len(df_result)):
        lista_pu.append([df_result.iloc[i,0], df_result.iloc[i,1]])

    return lista_pu
# Célula Principal
# Chama a função para buscar os Pu's no diretório
pu_diretorio(pu_diario_file)
# Chama a função para calcular os PU's da Cetip
cria_pu_cetip(pu_diario_file)

# # Chama a função para calcular os PU's das CRA's do dólar ======================== ERRO! ==========================
# pu_dolar(pu_diario_file)

# Converte a lista em um dataframe para ser salva na planilha
result = pd.DataFrame(lista_pu_geral, columns=["Cod Ativo", "PU"]) 
result["PU"] = result["PU"].astype(str)
result["PU"] = result["PU"].str.replace("R$", "")

print("\n =============================================\n\tArquivos processados com sucesso!\n =============================================\n")
dolar = extrai_dolar()
dolar
# Salva o resultado na aba "Resultado" do arquivo fonte dos Pu's
with pd.ExcelWriter(pu_diario_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:  
    result.to_excel(writer, sheet_name='Resultado')

print("\n =============================================================\n",data_atual_global,"\tResultado gravado na planilha de PU diário\n =============================================================\n")
