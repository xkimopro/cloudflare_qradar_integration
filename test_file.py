filename="cloudflare_log8.log"
fileobject=open(filename,"r")
fileobject=fileobject.readlines()
i=0
for event in fileobject:
    i=i+1 
    print event
    print i
