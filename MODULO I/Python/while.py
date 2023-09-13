"""
Repetições
while (enquanto)
Executa uma ação enquanto uma condição for verdadeira
Loop infinito -> Quando um código não tem fim
"""
contador = 0

while contador < 10:
    contador += 1

    if contador == 5:
        print('Não vou mostrar o 5.')
        continue

    if contador >= 7 and contador <= 9:
        print('Não vou mostrar o', contador)
        continue

    print(contador)

    if contador == 11:
        break


print('Acabou')