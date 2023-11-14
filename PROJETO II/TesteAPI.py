import requests
import pandas as pd
from IPython.display import display

api_key = "77f196738eb51a0a35a6211cc16a099e"
url = f"https://api.themoviedb.org/3/movie/top_rated?api_key={api_key}&amp;language=pt-BR"
# url_assistir = f" https://api.themoviedb.org/3/movie/238/watch/providers/"

response = requests.get(url)
data  = response.json()

print(data)

filmes = []
for movie in data['results']:
    df = {
    'Titulo': movie['title'],
    'Data lançamento': movie['release_date'],
    'Visão geral': movie['overview'],
    'Votos': movie['vote_count'],
    'Média de votos': movie['vote_average']
    }
    filmes.append(df)

df = pd.DataFrame(filmes)
display(df)
