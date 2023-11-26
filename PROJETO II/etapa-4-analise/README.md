# Etapa IV - Dashboard

### Inserindo Dados

**Com o banco de dados  *tl_refined_zone* pronto, realizei algumas querys no Athena, verificando se os dados estavam retornando em conformidade** 

![Consultas Athena](/img/consultas_athena.png)

Realizei algumas consultas e percebi que os dados estavam congruentes, o próximo passo foi inserir esses dados no QuickSight para a criação do DashBoard.

**Criei 3 conjunto de dados, planejando responder todas as questões definidas anteriormente no dashboard.**

![Conjunto Dados](/img/conjunto_dados.png)

* Analise_Filmes-Projeto-II - Onde se encontram todas as tabelas, que foram selecionados dessa maneira:

```sql
SELECT filme.*, filme_genero.id_genero, genero.genero, filme_produtora.id_produtora, produtora.produtora, filme_score.popularidade, filme_score.media_votos, filme_score.quantidade_votos, filme_lucro.orcamento, filme_lucro.receita, filme_lucro.lucro, filme_lucro.percent_lucro
FROM tl_refined_zone.filme
JOIN tl_refined_zone.filme_genero ON tl_refined_zone.filme.imdb_id = tl_refined_zone.filme_genero.imdb_id
JOIN tl_refined_zone.genero ON tl_refined_zone.filme_genero.id_genero = tl_refined_zone.genero.id_genero
JOIN tl_refined_zone.filme_produtora ON tl_refined_zone.filme.imdb_id = tl_refined_zone.filme_produtora.imdb_id
JOIN tl_refined_zone.produtora ON tl_refined_zone.filme_produtora.id_produtora = tl_refined_zone.produtora.id_produtora
LEFT JOIN tl_refined_zone.filme_score ON tl_refined_zone.filme_score.imdb_id = tl_refined_zone.filme_score.imdb_id
LEFT JOIN tl_refined_zone.filme_lucro ON tl_refined_zone.filme_lucro.imdb_id = tl_refined_zone.filme_lucro.imdb_id;
```

* Lucro_por_produtora - Onde somei o lucro de todos os filmes por produtora para trazer um gráfico afim de análisar quais produtoras lucraram mais com as atuações de Leonardo DiCaprio.

```sql
SELECT tl_refined_zone.filme_lucro.lucro, tl_refined_zone.produtora.produtora 
FROM tl_refined_zone.filme
JOIN tl_refined_zone.filme_lucro ON tl_refined_zone.filme.imdb_id = tl_refined_zone.filme_lucro.imdb_id
JOIN tl_refined_zone.filme_produtora ON tl_refined_zone.filme.imdb_id = tl_refined_zone.filme_produtora.imdb_id
JOIN tl_refined_zone.produtora ON tl_refined_zone.filme_produtora.id_produtora = tl_refined_zone.produtora.id_produtora;
```

* total_popularidade_produtora - Afim de trazer um gráfico somando a popularidade de todos os filmes por produtora e análisar qual produtora ganhou mais popularidade com as atuações do ator.

```sql
SELECT tl_refined_zone.produtora.produtora, tl_refined_zone.filme_score.popularidade AS Total_Popularidade
FROM tl_refined_zone.produtora
JOIN tl_refined_zone.filme_produtora ON tl_refined_zone.produtora.id_produtora = tl_refined_zone.filme_produtora.id_produtora
JOIN tl_refined_zone.filme ON tl_refined_zone.filme_produtora.imdb_id = tl_refined_zone.filme.imdb_id
JOIN tl_refined_zone.filme_score ON tl_refined_zone.filme.imdb_id = tl_refined_zone.filme_score.imdb_id
GROUP BY tl_refined_zone.produtora.produtora
ORDER BY Total_Popularidade DESC;
```

## Dashboard

**O Intuido do Dashboard é trazer visões de análise e comparações em relação aos filmes e produtoras em que o ator atuou ao longo do tempo, além de informações da tragetória e carreira do ator.**

* Atuações do ator, em relação a gênero e época e produtora.

* Relações de filmes do ator, popularidade, votações e lucratividade.

* Análise de popularidade e lucratividade referente a produtoras.

**Busquei explorar a ferramenta, utilizando filtros, gráficos e formatações visuais diferentes.**

É perceptível que a maioria dessas questões sejam melhor visualizadas com gráficos de barra, entretanto, procurei utilizar outros gráficos e tabelas afim de conseguir variedade de gráficos no dashboard.

### Resultado Final

- [Dashboard](/PROJETO%20II/etapa-4-analise/Analise_filmes_leonardoDiCaprio.pdf)

#### Considerações Finais

**Com o Dashboard pronto podemos notar algumas análises interessantes que eu gostaria de deixar registrado por aqui**

* É nítido como o sucesso do filme Titanic alavancou a carreira do ator nos filmes de Drama, o que o levou a atuar muito mais vezes nesse gênero.

* Mesmo com maiores lucros, o filme Titanic ainda perde para Inception em popularidade e votação popular.

* O Ator atuou o dobro de vezes para a Warner em relação a Paramount, apesar disso, a Paramount obteve bem mais lucros com os filmes do ator.

