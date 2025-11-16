import pyairbnb
import inspect

print("CONTENU DU MODULE pyairbnb :")
print(dir(pyairbnb))

print("\nCODE SOURCE DU MODULE pyairbnb :")
try:
    print(inspect.getsource(pyairbnb))
except:
    print("Impossible d'afficher le code source directement.")
