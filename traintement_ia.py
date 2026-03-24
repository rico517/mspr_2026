import mysql.connector
from mysql.connector import Error
import time
import pandas as pd


MAX_RETRIES = 5
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

cursor = mspr_conn.cursor()
cursor.execute("SELECT nom, prenom, label, tour, annee, type, code as code_circo, voix, abstentions,votants,exprimes,blancs_nuls,inscrits FROM votes JOIN scrutins_circonscriptions on scrutins_circonscriptions.id = votes.id_scrutin_circonscription join candidats on candidats.id = votes.id_candidat join bords_politiques on bords_politiques.id = candidats.id_bord_politique join scrutins on scrutins.id = scrutins_circonscriptions.id_scrutin join circonscriptions on circonscriptions.id = scrutins_circonscriptions.id_circonscription;")
df = pd.DataFrame(cursor.fetchall() , columns=[i[0] for i in cursor.description])

print(df.info())
print(df.head())