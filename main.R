# PIPELINE INTEGRADO PREVIC - ETAPAS N1, N2 E N3
library(dplyr); library(readr); library(stringr); library(fs); library(ggplot2); library(plotly)

# 1. CRIAR ARQUITETURA DE PASTAS (N3)
caminho_projeto <- file.path("C:/Users", Sys.getenv("USERNAME"), "Desktop", "PROJETO")
camadas <- c("01_Bronze", "02_Silver", "03_Gold")
sapply(file.path(caminho_projeto, camadas), function(p) if(!dir.exists(p)) dir_create(p))

# 2. FUNÇÃO DE TRATAMENTO TÉCNICO (N1)
# Converte "1.000,00" para 1000.00 para permitir cálculos [3]
limpar_financeiro <- function(col) {
  col %>% str_replace_all("\\.", "") %>% str_replace_all(",", ".") %>% as.numeric()
}

# 3. PROCESSAMENTO (BRONZE -> SILVER)
arquivos <- list.files(file.path(caminho_projeto, "01_Bronze"), pattern = "\\.csv$", full.names = TRUE)
base_lista <- list()

for (arq in arquivos) {
  # Detecção automática de separador (N1) [8]
  df <- read.csv(arq, sep = ",", encoding = "latin1", stringsAsFactors = FALSE) %>%
    mutate(across(starts_with("VL_"), ~replace(limpar_financeiro(.), is.na(.), 0)),
           DATA_COMP = as.Date(paste0("01/", DATA_COMP), format = "%d/%m/%Y"))
  base_lista[[basename(arq)]] <- df
}
base_consolidada <- bind_rows(base_lista)

# 4. MÉTRICAS E CAMADA GOLD (N2)
metricas_gold <- base_consolidada %>%
  group_by(SG_EFPC, DATA_COMP) %>%
  summarise(Patrimonio = sum(VL_SALDO_FINAL[NUM_CONTA == "2030000000000"], na.rm = TRUE),
            Provisoes = sum(VL_SALDO_FINAL[NUM_CONTA == "2030101000000"], na.rm = TRUE), .groups = "drop") %>%
  mutate(ISA = Patrimonio / Provisoes)

# 5. GERAR GRÁFICO INTERATIVO (Destaque do Projeto)
p <- ggplot(metricas_gold, aes(x = DATA_COMP, y = ISA, color = SG_EFPC, group = SG_EFPC)) +
     geom_line() + labs(title = "Evolução da Solvência Jan-Set/2025") + theme_minimal()
ggplotly(p) # Interatividade para navegar pelos trimestres

--------------------------------------------------------------------------------
