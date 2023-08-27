# Linux

## Diretórios

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

## Comandos do Terminal

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

## Criar, Alterar e Deletar

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

## Gerenciar pacotes

apt update -> Atualizar aplicativos

apt upgrade -> Atualizar pacotes

apt install -> Instalar pacote

apt remove -> Remover pacote

apt autoremove -> Remover pacotes desnecessários

apt-cache search -> Busca pacote inúteis

## Buscar em arquivos e diretórios

head -> Ver o topo de um arquivo

tail -> Ver final de um arquivo

grep -> pesquisar texto

grep -c -> ver repetições

find -> buscar arquivos e diretórios

find -name -> por nome

find -type f ou d -> arquivos ou diretorios

which -> Visualizar onde os comandos estão sendo executados

## Editores de texto

### Nano

Nano "file name" -> Abrir

Ctrl + O -> Salvar

Ctrl + X -> Fechar 

Ctrl + R -> Inserir arquivo

Ctrl + W -> Pesquisar texto

Alt + / -> Final do arquivo

Alt + \ -> Inicio do arquivo

Alt + g -> linha específica

### Vim 

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

## Gerenciar Usuários

adduser -> Criar usuário

userdel --remove -> Remover usuário

usermod -c -> Redefinir nome

usermod -L -> Desabilitar user

usermod -U -> Habilitar user

groupadd -g "id" "groupname" -> Criar grupo

getent group -> Ver grupos

group del "groupname" -> Deletar grupo

groups "username" -> Ver o grupo de um user

usermod -a -G "groupname" "user" -> Mover user

gpasswd -d "username" "groupname" -> Remover user de um grupo 

passwd -> Trocar senha 

## Gerenciar Permissões

- Leitura -> Ler arquivos (R - Read)
- Escrita -> Escrever em arquivos (W- Write)
- Execução -> Executar arquivos (X - Execute)

### Alterar permissões

- chmod "X X X" :File or :Dir

¹X - Dono

²X - Grupo

³X - Users

a -> all

u -> user/owner

g -> group

o -> outhers

***Exemplos***

chmod u=rx -> Seta permissões de leitura e execução para o User

chmod a-rwx -> Remove as permissões de All

chmod g+x -> Adiciona permissão de executar par Group

___

chown "user" "file ou dir" -> Alterar User

chown "user":"group" "file or dir" -> Alterar User e Group

chgrp "group" "file" -> Alterar grupo

history -> Ver comando utilizados 

## Compactar Arquivos

tar -czvf "name".tar.gz "arquivo" -> Compactar arquivos

tar -xzvf "arquivo" -c "diretorio de origem" -> Descompactar

zip -r "name" "arquivos" -> Compactar.zip

unzip "arquivo" -d "diretorio de origem" -> Descompactar.zip

## Gerenciamento de Redes

***DNS -> Domain Name System***

***UDP -> Mais velocidade utilizado para transmissões ao vivo e jogos***

***TCP -> Mais seguro, garante integridade dos dados***


**Portas**

20 -> FTP

22 -> SSH

80 -> HTTP

443 -> HTTPS

### Comandos 

netstat -> Visualizar conexões

netstat -at -> Conexões TCP

netstat -au -> Conexões UDP

neteat -> Fazer conexão com porta específica

ifconfig ->  Ip config

nslookup -> Ver ip mais próximo

tcpdump -> Ver conexões TCP

Hostname -I -> Ver IP




