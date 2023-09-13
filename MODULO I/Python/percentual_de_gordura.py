
#Não sabe seu IMC?
IMC_quest = input("Sabe seu IMC? ")
   
if IMC_quest == ("sim"):
    IMC = float(input("Digite seu IMC: "))
else:
    peso = float(input("Digite seu peso: "))
    altura = float(input("Digite sua altura em metros: "))
    IMC = float(peso / altura ** 2)
    print(f"Esse é seu IMC: {IMC}")

#Calcular percentual 
idade = float(input("Digite sua idade: "))
sexo_quest = input("Qual o seu sexo? (M ou F): ")

if sexo_quest == "M" or sexo_quest == "m":
    sexo = 1
elif sexo_quest == "F" or sexo_quest == "f":
    sexo = 0
else:
    print("Sexo inválido")
    exit()

#BT= (1,20 x IMC) + (0,23 x idade) – (10,8 x sexo) – 5.4
percentual = ((1.20 * IMC) + (0.23 * idade) - (10.8 * sexo)) - 5.4
print(f"Seu percentual de gordura é de: {percentual:.2f} %")