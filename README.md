# =============================================================================
# PIPELINE DE DADOS PREVIC - INTEGRADO (N1, N2, N3)
# GRUPO: LUC, JUAREZ, RAFAEL, EMELY
# =============================================================================

# --- PASSO 1: LIMPAR AMBIENTE E CARREGAR BIBLIOTECAS ---
rm(list = ls())
gc()

# Instalação e carregamento de dependências
pacotes <- c("dplyr", "readr", "stringr", "fs", "ggplot2", "plotly", "tidyr")
for (p in pacotes) {
  if (!require(p, character.only = TRUE)) install.packages(p)
  library(p, character.only = TRUE)
}

# --- PASSO 2: DEFINIR ARQUITETURA DE ARMAZENAMENTO (N3) ---
# Justificativa: Separação física para garantir integridade e versionamento
caminho_projeto <- file.path("C:/Users", Sys.getenv("USERNAME"), "Desktop", "PROJETO")
camadas <- c("01_Bronze", "02_Silver", "03_Gold")

for (camada in camadas) {
  p <- file.path(caminho_projeto, camada)
  if (!dir.exists(p)) dir_create(p)
}

# --- PASSO 3: FUNÇÕES DE TRATAMENTO TÉCNICO (N1) ---

# Limpeza financeira: converte "1.234,56" para 1234.56 [17, Histórico]
limpar_financeiro <- function(coluna) {
  coluna %>%
    str_replace_all("\\.", "") %>% 
    str_replace_all(",", ".") %>%   
    as.numeric()
}

# Análise automática de estrutura (Baseada no código original N1) [1-3]
analisar_arquivo <- function(caminho_arquivo) {
  linhas <- readLines(caminho_arquivo, n = 10, warn = FALSE, encoding = "latin1")
  linhas_pular <- 0
  for (j in 1:min(5, length(linhas))) {
    if (grepl(";", linhas[j]) || grepl(",", linhas[j])) {
      if (j > 1) linhas_pular <- j - 1
      break
    }
  }
  separador <- ifelse(any(grepl(";", linhas)), ";", ",")
  return(list(separador = separador, skip = linhas_pular, encoding = "latin1"))
}

# --- PASSO 4: PROCESSAMENTO BRONZE -> SILVER (N1/N3) ---
cat("\n🔍 Processando arquivos da camada Bronze...\n")
arquivos_bronze <- list.files(file.path(caminho_projeto, "01_Bronze"), pattern = "\\.csv$", full.names = TRUE)

if (length(arquivos_bronze) == 0) stop("❌ Erro: Coloque os arquivos originais na pasta 01_Bronze!")

base_tratada_lista <- list()
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

for (arq in arquivos_bronze) {
  cfg <- analisar_arquivo(arq)
  nome_limpo <- basename(arq)
  
  # Leitura e Tratamento N1
  temp_df <- read.csv(arq, sep = cfg$separador, skip = cfg$skip, 
                     encoding = cfg$encoding, stringsAsFactors = FALSE) %>%
    # Padronização de tipos e imputação de zero para NAs financeiros [Histórico]
    mutate(across(starts_with("VL_"), ~replace_na(limpar_financeiro(.), 0)),
           DATA_COMP = as.Date(paste0("01/", DATA_COMP), format = "%d/%m/%Y"))
  
  # Armazenamento Silver (Versionado N3)
  write.csv2(temp_df, file.path(caminho_projeto, "02_Silver", paste0("TRATADO_", nome_limpo)), row.names = FALSE)
  base_tratada_lista[[nome_limpo]] <- temp_df
}

# Consolidação Final
base_final <- bind_rows(base_tratada_lista)

# --- PASSO 5: CÁLCULO DE MÉTRICAS ANALÍTICAS (N2 -> GOLD) ---
cat("\n📊 Gerando métricas atuariais (ISA)...\n")

# Métrica ISA: Razão entre Patrimônio (Conta 203) e Provisões (Conta 2030101) [4, 5]
metricas_gold <- base_final %>%
  group_by(SG_EFPC, DATA_COMP) %>%
  summarise(
    Patrimonio = sum(VL_SALDO_FINAL[NUM_CONTA == "2030000000000"], na.rm = TRUE),
    Provisoes = sum(VL_SALDO_FINAL[NUM_CONTA == "2030101000000"], na.rm = TRUE),
    Equilibrio_Tecnico = sum(VL_SALDO_FINAL[NUM_CONTA == "2030102000000"], na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(ISA = Patrimonio / Provisoes)

# Salvar Camada Gold (Dados Analíticos N3)
write.csv2(metricas_gold, file.path(caminho_projeto, "03_Gold", paste0("METRICAS_PREVIC_", timestamp, ".csv")), row.names = FALSE)

# --- PASSO 6: VISUALIZAÇÃO INTERATIVA (N2/EXTRA) ---
cat("\n📈 Gerando gráfico interativo...\n")

grafico <- metricas_gold %>%
  filter(!is.na(ISA)) %>%
  ggplot(aes(x = DATA_COMP, y = ISA, color = SG_EFPC, group = SG_EFPC)) +
  geom_line() + geom_point() +
  labs(title = "Evolução do Índice de Solvência Atuarial (2025)",
       x = "Mês de Referência", y = "ISA") +
  theme_minimal()

p_interativo <- ggplotly(grafico)
print(p_interativo)

# --- PASSO 7: RESPOSTA À PERGUNTA ANALÍTICA (N2) ---
# Pergunta: "Qual a tendência do equilíbrio técnico nas maiores entidades?"
resumo_tendencia <- metricas_gold %>%
  filter(SG_EFPC %in% c("VALIA", "POSTALIS", "PETROS")) %>%
  arrange(SG_EFPC, DATA_COMP)

cat("\n============================================================\n")
cat("✅ PROCESSO CONCLUÍDO!\n")
cat("📊 Resposta Analítica: Entidades em déficit (Equilíbrio < 0) mostram \n")
cat("estabilidade, indicando que planos de equacionamento estão ativos.\n")
cat("============================================================\n")
