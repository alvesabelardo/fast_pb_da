* Foi criado um volume no Dockerfile "/data" onde foram upados os arquivos da pasta "dados"

* Em index.py os dados são puxados do volume do container "/data" e são enviados para o bucket.

* Executa uma tentativa de upload passando as variaveis com nome do bucket, caminho e nome do arquivo, e verifica se foram upados corretamente

* Os Arquivos de dados não podem ser upados no github pelo tamanho de armazenamento.