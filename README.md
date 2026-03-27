# GRUPO-LUC-RAFAEL-JUAREZ-EMELY
Previdência 
# =============================================================================
# PIPELINE DE DADOS PREVIC - CONSOLIDADO ETAPAS N1, N2 E N3
# GRUPO: LUC, JUAREZ, RAFAEL, EMELY
# =============================================================================

# --- PASSO 1: LIMPAR AMBIENTE E CARREGAR DEPENDÊNCIAS ---
rm(list = ls())
gc()

if (!require(dplyr)) install.packages("dplyr")
if (!require(readr)) install.packages("readr")
if (!require(stringr)) install.packages("stringr")
if (!require(fs)) install.packages("fs")
if (!require(ggplot2)) install.packages("ggplot2")

library(dplyr)
library(readr)
library(stringr)
library(fs)
library(ggplot2)

# --- PASSO 2: DEFINIR ARQUITETURA DE ARMAZENAMENTO (N3) ---
# Define o caminho base e cria as camadas físicas para evitar sobrescrita
caminho_projeto <- file.path("C:/Users", Sys.getenv("USERNAME"), "Desktop", "PROJETO")
camadas <- c("01_Bronze", "02_Silver", "03_Gold")

for (camada in camadas) {
  p <- file.path(caminho_projeto, camada)
  if (!dir.exists(p)) dir_create(p)
}

# --- PASSO 3: FUNÇÕES DE TRATAMENTO TÉCNICO (N1) ---

# Justificativa Técnica: Os arquivos usam o padrão "1.000,00". 
# Para cálculos no R, removemos o ponto e trocamos a vírgula por ponto decimal.
limpar_financeiro <- function(coluna) {
  coluna %>%
    str_replace_all("\\.", "") %>% 
    str_replace_all(",", ".") %>%   
    as.numeric()
}

# Função do Script N1 para detectar separador e encoding automaticamente
analisar_arquivo <- function(caminho_arquivo) {
  linhas <- readLines(caminho_arquivo, n = 10, warn = FALSE, encoding = "latin1")
  
  # Detectar se há metadados para pular (skip)
  linhas_pular <- 0
  for (j in 1:min(5, length(linhas))) {
    if (grepl(";", linhas[j]) || grepl(",", linhas[j])) {
      if (j > 1) linhas_pular <- j - 1
      break
    }
  }
  
  # Detectar separador
  separador <- ifelse(any(grepl(";", linhas)), ";", ",")
  
  return(list(separador = separador, skip = linhas_pular, encoding = "latin1"))
}

# --- PASSO 4: PROCESSAMENTO E TRATAMENTO (BRONZE -> SILVER) ---

cat("\n🔍 Escaneando arquivos na camada Bronze...\n")
todos_arquivos <- list.files(file.path(caminho_projeto, "01_Bronze"), pattern = "\\.csv$", full.names = TRUE)

if (length(todos_arquivos) == 0) stop("❌ Coloque os arquivos originais na pasta 01_Bronze!")

dados_consolidados <- list()
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

for (arq in todos_arquivos) {
  cfg <- analisar_arquivo(arq)
  nome_arq <- basename(arq)
  
  # Importação e Tratamento N1
  temp_df <- read.csv(arq, sep = cfg$separador, skip = cfg$skip, 
                     encoding = cfg$encoding, stringsAsFactors = FALSE) %>%
    # Correção de tipos e preenchimento de dados faltantes (Imputação zero)
    mutate(across(starts_with("VL_"), ~replace(limpar_financeiro(.), is.na(.), 0)),
           DATA_COMP = as.Date(paste0("01/", DATA_COMP), format = "%d/%m/%Y"))
  
  # Salva versão tratada na Silver com timestamp (Versionamento N3)
  write.csv2(temp_df, file.path(caminho_projeto, "02_Silver", paste0("TRATADO_", nome_arq)), row.names = FALSE)
  
  dados_consolidados[[nome_arq]] <- temp_df
}

# Unificação de todos os balancetes tratados
base_final <- bind_rows(dados_consolidados)

# --- PASSO 5: CÁLCULO DE MÉTRICAS ANALÍTICAS (N2 -> GOLD) ---

cat("\n📊 Gerando métricas analíticas na camada Gold...\n")

# Métrica: Índice de Solvência Atuarial (ISA) e Rentabilidade
# Justificativa: Cruzamento das contas de Patrimônio Social (203) e Provisões (2030101)
metricas_gold <- base_final %>%
  group_by(SG_EFPC, DATA_COMP) %>%
  summarise(
    Net_Worth = sum(VL_SALDO_FINAL[NUM_CONTA == "2030000000000"], na.rm = TRUE),
    Provisions = sum(VL_SALDO_FINAL[NUM_CONTA == "2030101000000"], na.rm = TRUE),
    Invest_Flow = sum(VL_SALDO_FINAL[NUM_CONTA == "5000000000000"], na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    ISA = Net_Worth / Provisions, # Índice de Solvência
    Rentabilidade = (Invest_Flow / Net_Worth) * 100
  )

# Salva resultado analítico na camada Gold
arquivo_gold <- file.path(caminho_projeto, "03_Gold", paste0("METRICAS_PREVIC_", timestamp, ".csv"))
write.csv2(metricas_gold, arquivo_gold, row.names = FALSE)

# --- PASSO 6: RESPOSTA À PERGUNTA ANALÍTICA (N2) ---
# Pergunta: "Como variou a solvência média das entidades no período?"
solvencia_media <- mean(metricas_gold$ISA, na.rm = TRUE)

cat("\n============================================================\n")
cat("✅ PROCESSO CONCLUÍDO COM SUCESSO!\n")
cat(paste0("📊 Solvência Média Geral: ", round(solvencia_media, 4), "\n"))
cat(paste0("🥈 Dados tratados salvos em: 02_Silver\n"))
cat(paste0("🥇 Métricas finais salvas em: 03_Gold\n"))
cat("============================================================\n")
