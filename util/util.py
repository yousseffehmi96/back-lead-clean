import unicodedata
import re
def NetoyerUneChaine(text:str)->str:
    ch=""
    if not text:
        return ""
    # sèparer la lettre est son accènt 
    nfd=unicodedata.normalize('NFD',text)
    for i in nfd:
        # determiner la category d'un caractère
        if unicodedata.category(i)!="Mn":
            ch=ch+i
    ch = re.sub(r'[^a-zA-Z0-9\s]', '', ch)
    ch=ch[0].upper()+ch[1:]
    return ch
def NetoyerUnNumero(text:str)->str:
    ch=""
    if not text:
        return ""
    # sèparer la lettre est son accènt 
    nfd=unicodedata.normalize('NFD',text)
    for i in nfd:
        # determiner la category d'un caractère
        if unicodedata.category(i)=="Nd":
            ch=ch+i
    return ch
def NettoyerUnEmail(text:str)->str:
    ch=""
    if not text:
        return ""
    # sèparer la lettre est son accènt 
    nfd=unicodedata.normalize('NFD',text)
    for i in nfd:
        # determiner la category d'un caractère
        if unicodedata.category(i)!="Mn":
            ch=ch+i
    ch = re.sub(r'[^a-zA-Z0-9@._-]', '', ch)
    ch = ch.strip()
    print(ch)
    if(ch.startswith("@")):
        ch=ch[1:]
   
    if(ch.endswith("@")):
        print('ffffffffffffffff')
        ch=ch[:len(ch)-1]
   
    return ch.lower()
    