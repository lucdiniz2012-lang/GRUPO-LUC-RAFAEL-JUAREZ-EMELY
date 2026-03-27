
# 📉 Pipeline Analítico PREVIC 2025: Solvência e Equilíbrio Técnico

Este projeto automatiza a ingestão, o tratamento técnico e a análise atuarial dos balancetes das EFPCs de **janeiro a setembro de 2025**.

## 🏗️ Arquitetura de Armazenamento (Medallion)
Implementamos uma estrutura de pastas físicas para garantir a linhagem dos dados e evitar a sobrescrita [1]:

- **01_Bronze (Raw):** Arquivos CSV originais da PREVIC (Latin-1) [2].
- **02_Silver (Treated):** Dados limpos com conversão de strings financeiras brasileiras para numérico real [3].
- **03_Gold (Analytical):** Métricas consolidadas (ISA e Equilíbrio Técnico) com versionamento por timestamp [4].

## 📊 Principais Descobertas
- **Índice de Solvência Atuarial (ISA):** Identificamos que entidades como a **VALIA** mantêm solvência robusta, enquanto outras operam em níveis de atenção.
- **Déficit Técnico:** Monitoramento das contas `2030102010200` para identificar planos sob equacionamento [5].

## 👥 Integrantes
LUC, JUAREZ, RAFAEL, EMELY

--------------------------------------------------------------------------------
