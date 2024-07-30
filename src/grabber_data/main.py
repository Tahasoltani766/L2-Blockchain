import numpy as np

result = np.array(
    [['0xC8d8E393825CCC50c07447ecC7767278F6318a82', '0', '11'],
     ['0xC8d8E393825CCC50c07447ecC7767278F6318a82', '0', '5'],
     ['0xC8d8E393825CCC50c07447ecC7767278F6318a82', '0', '3'],
     ['0xD90A698a012F4a785E4405e415611d3805ffcF7f', '0', '7'],
     ['0x4d646258983E741eD3228f43579ba5E827008e9F', '0', '7'],
     ['0x103e37ad040437420f53c1c5a2869AC1D6149331', '0', '7']])


def insert_balance(x, v, g):
    print(x, v, g)


for item in result:
    insert_balance(item[0], item[1], item[2])
