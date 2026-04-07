# Estrutura de Relatórios (Reports)

A partir de agora, todos os arquivos CSV de auditoria e relatórios gerados pelos scripts serão organizados em uma estrutura de diretórios padronizada sob `reports/`.

## Estrutura de Diretórios

```
reports/
├── move_duplicate_deals/
│   └── YYYY/MM/
│       └── move_duplicate_deals_{pipeline_id}_{timestamp}.csv
├── delete_duplicate_deals/
│   └── YYYY/MM/
│       └── delete_duplicate_deals_{timestamp}.csv
├── delete_orphan_deals/
│   └── YYYY/MM/
│       └── delete_orphan_deals_{timestamp}.csv
└── delete_contacts/
    └── YYYY/MM/
        └── delete_contacts_{timestamp}.csv
```

### Explicação

- **Nível 1**: Tipo de operação (`move_duplicate_deals`, `delete_duplicate_deals`, etc.)
- **Nível 2-3**: Organização por data (`YYYY/MM`)
- **Nível 4**: Nome do arquivo com timestamp (`YYYYMMDD_HHMMSS`)

## Scripts e Seus Relatórios

### 1. `ploomes_move_duplicate_deals.py`
- **Relatório**: `reports/move_duplicate_deals/YYYY/MM/move_duplicate_deals_{pipeline_id}_{timestamp}.csv`
- **Conteúdo**: Deal ID, Stage ID anterior, CNJ, Produto, Data de criação, Status
- **Exemplo**: `reports/move_duplicate_deals/2026/04/move_duplicate_deals_110067326_20260407_143022.csv`

### 2. `ploomes_delete_duplicate_deals.py`
- **Relatório**: `reports/delete_duplicate_deals/YYYY/MM/delete_duplicate_deals_{timestamp}.csv`
- **Conteúdo**: Deal ID, CNJ, Produto, Data de criação
- **Exemplo**: `reports/delete_duplicate_deals/2026/04/delete_duplicate_deals_20260407_143022.csv`

### 3. `ploomes_delete_orphan_deals.py`
- **Relatório**: `reports/delete_orphan_deals/YYYY/MM/delete_orphan_deals_{timestamp}.csv`
- **Conteúdo**: Deal ID, Status (ok, not_found, error)
- **Exemplo**: `reports/delete_orphan_deals/2026/04/delete_orphan_deals_20260407_143022.csv`
- **Nota**: Este script lê `orphan_deals.csv` como entrada

### 4. `ploomes_delete_contacts.py`
- **Relatório**: `reports/delete_contacts/YYYY/MM/delete_contacts_{timestamp}.csv`
- **Conteúdo**: Contact ID, Status (ok, not_found, error)
- **Exemplo**: `reports/delete_contacts/2026/04/delete_contacts_20260407_143022.csv`

## Benefícios da Nova Estrutura

✅ **Organização clara**: Relatórios agrupados por tipo de operação
✅ **Histórico rastreável**: Arquivos organizados por data (ano/mês)
✅ **Fácil limpeza**: Relatórios antigos podem ser arquivados por data
✅ **Escalabilidade**: Estrutura suporta crescimento sem desorganização
✅ **Auditoria**: Trailing completo de todas as operações executadas

## Implementação Técnica

A nova estrutura é gerenciada pelo módulo `ploomes/report_manager.py`:

```python
from ploomes.report_manager import ReportManager

# Uso básico
manager = ReportManager("move_duplicate_deals", identifier=110067326)
path = manager.get_full_path()  # Cria diretórios e retorna caminho completo

# Ou sem identificador
manager = ReportManager("delete_orphan_deals")
path = manager.get_full_path()
```

O `ReportManager` automaticamente:
1. Cria a estrutura de diretórios (ano/mês)
2. Gera o caminho completo do arquivo CSV
3. Include timestamp no nome do arquivo para garantir unicidade

## Migração de Arquivos Antigos

Se necessário, mova arquivos CSV antigos para a estrutura de relatórios:

```bash
# Exemplo: mover relatórios de moves duplicados
mkdir -p reports/move_duplicate_deals/2026/04
mv moved_duplicate_deals_*.csv reports/move_duplicate_deals/2026/04/

# Exemplo: mover relatórios de deleções
mkdir -p reports/delete_duplicate_deals/2026/04
mv deleted_duplicate_deals.csv reports/delete_duplicate_deals/2026/04/
```

## Próximas Etapas (Opcional)

- [ ] Adicionar rotação automática de relatórios antigos
- [ ] Criar script para gerar relatórios consolidados por período
- [ ] Adicionar compressão de relatórios antigos
- [ ] Configurar backup automático para S3 ou Azure Blob Storage
