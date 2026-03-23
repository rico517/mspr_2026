import pandas as pd
import seaborn as sns
import mysql.connector
from mysql.connector import Error
import time

test = pd.read_csv("./resultats-par-niveau-burvot-t1-france-entiere.csv",sep=";",low_memory=False, decimal=',')
test1 = pd.read_csv("./resultats-par-niveau-burvot-t2-france-entiere.csv",sep=";",low_memory=False, decimal=',')
test2 = pd.read_csv("./PR17_BVot_T1_FE.csv",sep=";",low_memory=False, decimal=',')
test3 = pd.read_csv("./PR17_BVot_T2_FE.csv",sep=";",low_memory=False, decimal=',')

df_filtre = test[test['Libelle du departement'].str.contains('Paris',case=False)]
df_filtre1 = test1[test1['Libelle du departement'].str.contains('Paris',case=False)]
df_filtre2 = test2[test2['Libelle du departement'].str.contains('Paris',case=False)]
df_filtre3 = test3[test3['Libelle du departement'].str.contains('Paris',case=False)]

# Nombre maximum de tentatives de connexion à la base de données
MAX_RETRIES = 5
# Délai entre les tentatives (en secondes)
RETRY_DELAY = 10
def connect_to_database(host, port, user, password, database, retries=MAX_RETRIES):
    """
    Établit une connexion à la base de données avec mécanisme de retry.

    Args:
        host: Hôte de la base de données
        port: Port de la base de données
        user: Nom d'utilisateur
        password: Mot de passe
        database: Nom de la base de données
        retries: Nombre de tentatives

    Returns:
        Connexion à la base de données

    Raises:
        Exception: Si la connexion échoue après toutes les tentatives
    """
    for attempt in range(retries):
        try:
            print(f"Tentative de connexion à {database} ({attempt + 1}/{retries})...")
            conn = mysql.connector.connect(
                host=host, port=port,
                user=user, password=password, database=database,
                connect_timeout=30,auth_plugin='mysql_native_password'
            )
            print(f"Connexion à {database} établie avec succès")
            return conn
        except Error as e:
            print(f"Erreur de connexion à {database}: {str(e)}")
            if attempt < retries - 1:
                print(f"Nouvelle tentative dans {RETRY_DELAY} secondes...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Échec de connexion à {database} après {retries} tentatives")
                raise
db_host ='localhost'
db_port = 3306
db_user = 'root'
db_password = 'root'

mspr_conn = connect_to_database(
            host=db_host, port=db_port,
            user=db_user, password=db_password,
            database='elections_db'
        )

a_sommer=['Inscrits','Abstentions','Votants','Blancs','Nuls','Exprimes','Voix1','Voix2','Voix3','Voix4','Voix5','Voix6','Voix7','Voix8','Voix9','Voix10','Voix11','Voix12']
a_sommer1=['Inscrits','Abstentions','Votants','Blancs','Nuls','Exprimes','Voix1','Voix2']
a_sommer2=['Inscrits','Abstentions','Votants','Blancs','Nuls','Exprimes','Voix1','Voix2','Voix3','Voix4','Voix5','Voix6','Voix7','Voix8','Voix9','Voix10','Voix11']
a_sommer3=['Inscrits','Abstentions','Votants','Blancs','Nuls','Exprimes','Voix1','Voix2']

df_filtre_copie = df_filtre
df_filtre['Nom1']= df_filtre['Nom']
df_filtre['Prenom1']=df_filtre['Prenom']
df_filtre['Voix1']=df_filtre['Voix']
df_filtre=df_filtre.drop(['Sexe','Sexe2','Sexe3','Sexe4','Sexe5','Sexe7','Sexe6','Sexe8','Sexe9','Sexe10','Sexe11','Sexe12','No Panneau','No Panneau2','No Panneau3','No Panneau4','No Panneau5','No Panneau6','No Panneau7','No Panneau8','No Panneau9','No Panneau10','No Panneau11','No Panneau12','Code de la commune','Libelle de la commune','Code du b.vote','% Voix/Ins','% Voix/Ins2','% Voix/Ins3','% Voix/Ins4','% Voix/Ins5','% Voix/Ins6','% Voix/Ins7','% Voix/Ins8','% Voix/Ins9','% Voix/Ins10','% Voix/Ins11','% Voix/Ins12'], axis=1)
df_resultat = df_filtre.groupby(['Libelle de la circonscription','Code du departement','Libelle du departement','Code de la circonscription','Nom1','Prenom1','Nom2','Prenom2','Nom3','Prenom3','Nom4','Prenom4','Nom5','Prenom5','Nom6','Prenom6','Nom7','Prenom7','Nom8','Prenom8','Nom9','Prenom9','Nom10','Prenom10','Nom11','Prenom11','Nom12','Prenom12'])[a_sommer].sum().reset_index()
df_resultat['Blancs/Nuls'] = df_resultat['Blancs']+df_resultat['Nuls']
df_resultat=df_resultat.drop(['Blancs','Nuls'],axis=1)

df_filtre1['Nom1']=df_filtre1['Nom']
df_filtre1['Prenom1']=df_filtre1['Prenom']
df_filtre1['Voix1']=df_filtre1['Voix']
df_filtre1=df_filtre1.drop(['Sexe','Sexe2','No Panneau','No Panneau2','Code de la commune','Libelle de la commune','Code du b.vote','% Voix/Ins','% Voix/Ins2'], axis=1)
df_resultat1 = df_filtre1.groupby(['Libelle de la circonscription','Code du departement','Libelle du departement','Code de la circonscription','Nom1','Prenom1','Nom2','Prenom2'])[a_sommer1].sum().reset_index()
df_resultat1['Blancs/Nuls'] = df_resultat1['Blancs']+df_resultat1['Nuls']
df_resultat1=df_resultat1.drop(['Blancs','Nuls'],axis=1)

df_filtre2['Nom1']=df_filtre2['Nom']
df_filtre2['Prenom1']=df_filtre2['Prenom']
df_filtre2['Voix1']=df_filtre2['Voix']
df_filtre2=df_filtre2.drop(['Sexe','Sexe2','Sexe3','Sexe4','Sexe5','Sexe7','Sexe6','Sexe8','Sexe9','Sexe10','Sexe11','No Panneau','No Panneau2','No Panneau3','No Panneau4','No Panneau5','No Panneau6','No Panneau7','No Panneau8','No Panneau9','No Panneau10','No Panneau11','Code de la commune','Libelle de la commune','Code du b.vote','% Voix/Ins','% Voix/Ins2','% Voix/Ins3','% Voix/Ins4','% Voix/Ins5','% Voix/Ins6','% Voix/Ins7','% Voix/Ins8','% Voix/Ins9','% Voix/Ins10','% Voix/Ins11'], axis=1)
df_resultat2 = df_filtre2.groupby(['Libelle de la circonscription','Code du departement','Libelle du departement','Code de la circonscription','Nom1','Prenom1','Nom2','Prenom2','Nom3','Prenom3','Nom4','Prenom4','Nom5','Prenom5','Nom6','Prenom6','Nom7','Prenom7','Nom8','Prenom8','Nom9','Prenom9','Nom10','Prenom10','Nom11','Prenom11'])[a_sommer2].sum().reset_index()
df_resultat2['Blancs/Nuls'] = df_resultat2['Blancs']+df_resultat2['Nuls']
df_resultat2=df_resultat2.drop(['Blancs','Nuls'],axis=1)

df_filtre3['Nom1']=df_filtre3['Nom']
df_filtre3['Prenom1']=df_filtre3['Prenom']
df_filtre3['Voix1']=df_filtre3['Voix']
df_filtre3=df_filtre3.drop(['Sexe','Sexe2','No Panneau','No Panneau2','Code de la commune','Libelle de la commune','Code du b.vote','% Voix/Ins','% Voix/Ins2'], axis=1)
df_resultat3 = df_filtre3.groupby(['Libelle de la circonscription','Code du departement','Libelle du departement','Code de la circonscription','Nom1','Prenom1','Nom2','Prenom2'])[a_sommer3].sum().reset_index()
df_resultat3['Blancs/Nuls'] = df_resultat3['Blancs']+df_resultat3['Nuls']
df_resultat3=df_resultat3.drop(['Blancs','Nuls'],axis=1)

df_resultat['Bord1']=df_resultat['Nom1'].replace('ARTHAUD','EXG')
df_resultat['Bord2']=df_resultat['Nom2'].replace('ROUSSEL','COM')
df_resultat['Bord3']=df_resultat['Nom3'].replace('MACRON','C')
df_resultat['Bord4']=df_resultat['Nom4'].replace('LASSALLE','CD')
df_resultat['Bord5']=df_resultat['Nom5'].replace('LE PEN','EXD')
df_resultat['Bord6']=df_resultat['Nom6'].replace('ZEMMOUR','EXD')
df_resultat['Nom7']=df_resultat['Nom7'].replace('MeLENCHON','MELENCHON')
df_resultat['Bord7']=df_resultat['Nom7'].replace('MELENCHON','RG')
df_resultat['Bord8']=df_resultat['Nom8'].replace('HIDALGO','G')
df_resultat['Bord9']=df_resultat['Nom9'].replace('JADOT','G')
df_resultat['Nom10']=df_resultat['Nom10'].replace('PeCRESSE','PECRESSE')
df_resultat['Bord10']=df_resultat['Nom10'].replace('PECRESSE','D')
df_resultat['Bord11']=df_resultat['Nom11'].replace('POUTOU','EXG')
df_resultat['Bord12']=df_resultat['Nom12'].replace('DUPONT-AIGNAN','SOU')

df_resultat['Annee']=df_resultat['Nom12'].replace('DUPONT-AIGNAN',2022)
df_resultat['Tour']=df_resultat['Nom12'].replace('DUPONT-AIGNAN',1)

df_resultat1['Bord1']=df_resultat1['Nom1'].replace('MACRON','C')
df_resultat1['Bord2']=df_resultat1['Nom2'].replace('LE PEN','EXD')

df_resultat1['Annee']=df_resultat1['Nom2'].replace('LE PEN',2022)
df_resultat1['Tour']=df_resultat1['Nom2'].replace('LE PEN',2)

df_resultat2['Bord5']=df_resultat2['Nom5'].replace('ARTHAUD','EXG')
df_resultat2['Bord10']=df_resultat2['Nom10'].replace('ASSELINEAU','SOU')
df_resultat2['Bord3']=df_resultat2['Nom3'].replace('MACRON','C')
df_resultat2['Bord8']=df_resultat2['Nom8'].replace('LASSALLE','CD')
df_resultat2['Bord2']=df_resultat2['Nom2'].replace('LE PEN','EXD')
df_resultat2['Bord7']=df_resultat2['Nom7'].replace('CHEMINADE','SOU')
df_resultat2['Nom9']=df_resultat2['Nom9'].replace('MeLENCHON','MELENCHON')
df_resultat2['Bord9']=df_resultat2['Nom9'].replace('MELENCHON','RG')
df_resultat2['Bord4']=df_resultat2['Nom4'].replace('HAMON','G')
df_resultat2['Bord11']=df_resultat2['Nom11'].replace('FILLON','D')
df_resultat2['Bord6']=df_resultat2['Nom6'].replace('POUTOU','EXG')
df_resultat2['Bord1']=df_resultat2['Nom1'].replace('DUPONT-AIGNAN','SOU')

df_resultat2['Annee']=df_resultat2['Nom1'].replace('DUPONT-AIGNAN',2017)
df_resultat2['Tour']=df_resultat2['Nom1'].replace('DUPONT-AIGNAN',1)

df_resultat3['Bord1']=df_resultat3['Nom1'].replace('MACRON','C')
df_resultat3['Bord2']=df_resultat3['Nom2'].replace('LE PEN','EXD')

df_resultat3['Annee']=df_resultat3['Nom2'].replace('LE PEN',2017)
df_resultat3['Tour']=df_resultat3['Nom2'].replace('LE PEN',2)

cursor = mspr_conn.cursor()
list_df=[df_resultat,df_resultat1,df_resultat2,df_resultat3]
def requete_base():
    cursor.execute("INSERT INTO scrutins (type,annee,tour) values ('Presidentielle',2022,1),('Presidentielle',2022,2),('Presidentielle',2017,1),('Presidentielle',2017,2)")
    for i in range(1,19):
        cursor.execute("INSERT INTO circonscriptions (code) values (%s)",[i]) 
    mspr_conn.commit()
    for i in list_df : 
        for index, row in i.iterrows():
            carac1=(row['Libelle de la circonscription'][0])
            #print(type(carac1))
            carac2=(row['Libelle de la circonscription'][1])
            #print(type(carac2))
            caractotal=''
            if carac2 !='e':
                caractotal=carac1+carac2
            else:
                caractotal=carac1
            cursor.execute("INSERT INTO scrutins_circonscriptions (id_scrutin, id_circonscription, inscrits, abstentions, votants, exprimes, blancs_nuls) values((select id from scrutins where type like 'Presidentielle' and annee = %s and tour = %s),(select id from circonscriptions where code = %s), %s, %s, %s,%s,%s)",[row['Annee'],row['Tour'],caractotal,row['Inscrits'],row['Abstentions'],row['Votants'],row['Exprimes'],row['Blancs/Nuls']])
            mspr_conn.commit()
    cursor.execute("INSERT INTO bords_politiques (label) values ('EXG'),('RG'),('COM'),('G'),('CG'),('C'),('EXD'),('RD'),('SOU'),('D'),('CD')")
    mspr_conn.commit()
    for i in list_df:
        row = i.iloc[0]
        if row["Tour"]==2:
            for j in range(1,3):
                cursor.execute("INSERT IGNORE into candidats (id_bord_politique, nom, prenom) VALUES ((select id from bords_politiques where label LIKE %s),%s,%s);",[row["Bord"+str(j)],row["Nom"+str(j)],row["Prenom"+str(j)]])
            mspr_conn.commit()
        elif row["Annee"]==2022:
            for j in range(1,13):
                cursor.execute("INSERT IGNORE into candidats (id_bord_politique, nom, prenom) VALUES ((select id from bords_politiques where label LIKE %s),%s,%s);",[row["Bord"+str(j)],row["Nom"+str(j)],row["Prenom"+str(j)]])
            mspr_conn.commit()
        elif row["Annee"]==2017:
            for j in range(1,12):
                cursor.execute("INSERT IGNORE into candidats (id_bord_politique, nom, prenom) VALUES ((select id from bords_politiques where label LIKE %s),%s,%s);",[row["Bord"+str(j)],row["Nom"+str(j)],row["Prenom"+str(j)]])
            mspr_conn.commit()
        else:
            return "Erreur"
    for i in list_df:
        for index, row in i.iterrows():
            if row["Tour"]==2:
                carac1=(row['Libelle de la circonscription'][0])
                #print(type(carac1))
                carac2=(row['Libelle de la circonscription'][1])
                #print(type(carac2))
                caractotal=''
                if carac2 !='e':
                    caractotal=carac1+carac2
                else:
                    caractotal=carac1
                for j in range(1,3):
                    cursor.execute("INSERT INTO `votes`(`id_candidat`, `id_scrutin_circonscription`, `voix`) VALUES ((select id from candidats where nom LIKE %s and prenom LIKE %s),(SELECT id from scrutins_circonscriptions where id_scrutin = (SELECT id from scrutins where annee = %s and tour = %s) and id_circonscription = (select id from circonscriptions where code= %s ) ),%s);",[row["Nom"+str(j)],row["Prenom"+str(j)],row["Annee"],row["Tour"],caractotal,row["Voix"+str(j)]])
                mspr_conn.commit()
            elif row["Annee"]==2022:
                carac1=(row['Libelle de la circonscription'][0])
                #print(type(carac1))
                carac2=(row['Libelle de la circonscription'][1])
                #print(type(carac2))
                caractotal=''
                if carac2 !='e':
                    caractotal=carac1+carac2
                else:
                    caractotal=carac1
                for j in range(1,13):
                    cursor.execute("INSERT INTO `votes`(`id_candidat`, `id_scrutin_circonscription`, `voix`) VALUES ((select id from candidats where nom LIKE %s and prenom LIKE %s),(SELECT id from scrutins_circonscriptions where id_scrutin = (SELECT id from scrutins where annee = %s and tour = %s) and id_circonscription = (select id from circonscriptions where code= %s ) ),%s);",[row["Nom"+str(j)],row["Prenom"+str(j)],row["Annee"],row["Tour"],caractotal,row["Voix"+str(j)]])
                mspr_conn.commit()
            elif row["Annee"]==2017:
                carac1=(row['Libelle de la circonscription'][0])
                #print(type(carac1))
                carac2=(row['Libelle de la circonscription'][1])
                #print(type(carac2))
                caractotal=''
                if carac2 !='e':
                    caractotal=carac1+carac2
                else:
                    caractotal=carac1
                for j in range(1,12):
                    cursor.execute("INSERT INTO `votes`(`id_candidat`, `id_scrutin_circonscription`, `voix`) VALUES ((select id from candidats where nom LIKE %s and prenom LIKE %s),(SELECT id from scrutins_circonscriptions where id_scrutin = (SELECT id from scrutins where annee = %s and tour = %s) and id_circonscription = (select id from circonscriptions where code= %s ) ),%s);",[row["Nom"+str(j)],row["Prenom"+str(j)],row["Annee"],row["Tour"],caractotal,row["Voix"+str(j)]])
                mspr_conn.commit()
            else:
                return "Erreur"
requete_base()
#INSERT INTO `votes`(`id_candidat`, `id_scrutin_circonscription`, `voix`) VALUES ((select id from candidats where nom LIKE %s and prenom LIKE %s),(SELECT id from scrutins_circonscriptions where id_scrutin = (SELECT id from scrutins where annee = %s and tour = %s) and id_circonscription = (select id from circonscriptions where code= %s ) ),%s)