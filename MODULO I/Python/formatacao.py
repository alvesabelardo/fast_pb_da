a = 'AAAAA'
b = 'BBBBBB'
c = 1.1

string = 'b={nome2} a={nome1} a={nome1} c={nome3:.2f}'
formato = string.format(
    nome1=a, nome2=b, nome3=c
)

print(formato)

nome = 'Pedro'
sobrenome = 'Augusto'
idade = '13'

frase = f'{idade} anos tem {nome} {sobrenome}'
formatacao = frase.format(idade, nome, sobrenome)
print(formatacao)