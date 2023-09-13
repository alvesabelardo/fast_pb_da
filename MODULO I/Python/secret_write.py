import os

palavra = ('carreta')
acertos = ''
tentativas = 0

while True:
    letra = input('Digite uma letra: ')
    tentativas += 1
    print(f'Tentativa número: {tentativas}')

    if len(letra) > 1:
        print('Digite apenas uma letra.')
        continue
    
    if letra in palavra:
        acertos += letra
    
    palavra_formada = ''

    for letra_secreta in palavra:
        if letra_secreta in acertos:
            palavra_formada += letra_secreta
        else:
            palavra_formada += '*'
    
    print('palavra_formada: ', palavra_formada)

    if palavra_formada == palavra:
        os.system('clear')
        print('Você ganhou parabéns!')
        print(10 * '-')
        print(f'A palavra é {palavra.upper()}')
        print(10 * '-')
        print(f'Você acertou com: {tentativas} tentativas!')
        print(10 * '-')

    
