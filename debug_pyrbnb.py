import pyrbnb
import inspect

print("CONTENU DU MODULE pyrbnb :")
print(dir(pyrbnb))

print("\nCODE SOURCE DU MODULE :")
try:
    print(inspect.getsource(pyrbnb))
except:
    print("Impossible d'afficher le code source directement.")
