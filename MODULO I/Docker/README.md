# Docker

- Docker é um software que reduz a complexidade de setup de aplicações
- Proporciona mais velocidade na configuração do ambiente
- Melhora a perfomace na execução da aplicação 

## Containers

- Um pacote de código que pode executar uma imagem
- Multiplos containers podem rodar juntos com compose
- Imagem e container são recursos fundamentais

## Comandos

-> docker run       (criar/rodar um container)

-> docker ps        (ver containers rodando)

-> docker ps -d -p          ("-d" rodar em segundo plano "-p" escolher portas)

-> docker stop      (parar containers)

-> docker start     (iniciar containers)

-> docker run --name        (criar setando um nome)

-> docker logs "container name"     (ver logs)

-> docker rm        (deletar container)


### Imagem

- Arquivos programados para que o docker crie uma estrutura.

-> docker build .       (criar imagem)

-> docker image -ls     (visualizar imagens)

-> docker rmi   "image"     (remover imagens)

-> docker system prune      (remover imagens, containers e networks que não estão em uso)

-> docker run --rm         (remover containers apos o uso)

-> docker top "container"       (informações do container)

-> docker stats      (verificar processamento)

-> docker push "imagem"     (publicar imagem no docker hub)

## Volumes

- Uma forma prática de guardar dados persistentimente em aplicações, sem depender de containers

-> docker volume create "nome"      (criar volume)

-> docker volume inspect "nome

-> docker volume rm "nome"

-> docker volume prune

## Networks

- Uma forma de gerenciar conexões do Docker com outras plataformas

### Conexões

- Externa: Conexão com uma API em servidor remoto
- Host: Comunicação com o host
- Entre containers 

#### Comandos 

-> docker network ls

-> docker create -d "tipo de rede" "nome"   **Bridge ou mcvlan**

-> docker network rm "nome"

-> docker network connect "nome" "idcontainer"

-> docker network disconnect "nome" "idcontainer"

-> docker network ls

-> docker network inspect "nome"

## Docker Compose

- Rodar multiplos containers de uma vez

-> docker compose up -d 

-> docker compose down

