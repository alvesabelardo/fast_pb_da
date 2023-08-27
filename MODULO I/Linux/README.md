# Linux

### Diretórios

bin -> Arquivos binários

boot -> Inicialização do sistema

dev -> Arquivos entrada e saída dispositivos

etc -> Configuração de app's

home -> Usuários

lib -> Biblioteca

media -> Pastas de dispositivos

opt -> App's de terceiro

sbin -> Binários de inicialização

tmp -> Temporarios 

usr -> Modo de leitura 

var -> Log's

### Comandos do Terminal

cd -> Mudar de diretórios

cd- -> Último diretório aberto

cd~ -> Home 

ls -> Visualizar diretórios e arquivos

ls -i -> Arquivos com detalhes

ls -a -> Arquivos ocultos

ls -h -> Tamanho de arquivos

ls -R -> Subdiretórios

cat -> Visualizar conteúdo de arquivo

cat -n -> Visualizar em linhas 

touch -> Criar arquivo

man -> Visualizar manual de comandos

ctrl + r -> Pesquisa comandos já utilizados

#### Criar, Alterar e Deletar

mkdir -> Criar diretório

mkdir -v -> Ver os logs do comando

mkdir -p -> Criar diretórios dentro do diretórios

rm -> Remove 

rm -dv -> Remove diretórios vazios 

rm -rfv -> Remove diretórios com arquivos 

rmdir -> Remove diretórios

cp -> Copiar arquivos

cp -r /* -> Copiar diretório com todos os arquivos

mv -> Mover arquivos de diretórios

pwd -> Visualizar caminho da pasta atual

#### Gerenciar pacotes

apt update -> Atualizar aplicativos

apt upgrade -> Atualizar pacotes

apt install -> Instalar pacote

apt remove -> Remover pacote

apt autoremove -> Remover pacotes desnecessários

apt-cache search -> Busca pacote inúteis

#### Buscar em arquivos e diretórios

head -> Ver o topo de um arquivo

tail -> Ver final de um arquivo

grep -> pesquisar texto

grep -c -> ver repetições

find -> buscar arquivos e diretórios

find -name -> por nome

find -type f ou d -> arquivos ou diretorios

which -> Visualizar onde os comandos estão sendo executados

#### Editores de texto

##### Nano

Nano "file name" -> Abrir

Ctrl + O -> Salvar

Ctrl + X -> Fechar 

Ctrl + R -> Inserir arquivo

Ctrl + W -> Pesquisar texto

Alt + / -> Final do arquivo

Alt + \ -> Inicio do arquivo

Alt + g -> linha específica

##### Vim 

Vim "file name" -> Abrir

I -> Insert 

Esc -> Sair do Insert

:x -> Salvar e sair

:w -> Salvar 

:q -> Sair 

DD -> Deletar linha

U -> Ctrl + Z

/ -> Buscar -> "N" Avança e "Shift N" Volta

:g! -> Sair sem salvar

