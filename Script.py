# %%
import pandas as pd
import pygsheets
import holidays
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os


# %%
# ORDI FIXE Authentifier avec le fichier de clé JSON
#gc = pygsheets.authorize(service_file=r'D:\DATA\2024-11-2022_Tableau-Heures\feuille-heures-c0ab0678243d.json')

# %%
#PC PORTABLE
# Obtenir le chemin absolu de la racine du projet
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Construire un chemin relatif depuis la racine
token_sheet = os.path.join(PROJECT_ROOT, 'feuille-heures-c0ab0678243d.json')
#Demande d'accès a sheet
gc = pygsheets.authorize(service_file= token_sheet)

# %%
# Ouvrir la feuille Google Sheet par son titre
#spreadsheet = gc.open('2024-2026_TABLEAU DES HEURES')
spreadsheet = gc.open("RELEVE DES HEURES (REPONSES/NE PAS MODIFIER/FILTRER) ")

# %%
# Ouvrir la première feuille (DATA)
worksheet = spreadsheet.sheet1

# %%
#Importer toute les valeur et les convertir dans un df pandas
values = worksheet.get_all_values()
df = pd.DataFrame(values[1:], columns=values[0])


# %%
df = df.replace('', pd.NA)
df.tail(5)

# %%
df.head()

# %%
#Supprimer lmes ligne sans valeur dans les colonnes Prénom Nom, Date
df = df.dropna(subset =["Prénom, NOM", "DATE"], how ="all").reset_index(drop =True)
print("Nombre de ligne conservées =",len(df))

# %%
#PREFILTRAGE DES DATE : Convertis la colone timestamp pour reduire la taille du df car la colonne date est trop hétérogène
df["Timestamp"] = pd.to_datetime(df["Timestamp"],format='mixed')

#Créer une colonne anné a partir de timestamp puis nous réduison la taille du DF a partir des date supérieur ou égale 2023
df["Annee_timestamp"] = df["Timestamp"].dt.year
df = df[df["Annee_timestamp"] >= 2023]
print("Nombre de ligne conservées =",len(df))

# %%
#Modifie les années abhérante observée dans date 0024 et 2004
df["DATE"] = df["DATE"].apply(lambda x : x.replace("0024", "2024").replace("2004", "2024"))

# %%
#Transforme la colonne date en datetime
df["DATE"] = pd.to_datetime(df["DATE"])

# %%
#Créé une colonne année, mois, n) de semaine a partir de la colonne DATE, les ligne 2022 sont peu donc conservées
df["Annee"] = df["DATE"].dt.year
df["Mois"] = df["DATE"].dt.strftime('%m-%B')
df["N_semaine"] = df["DATE"].dt.isocalendar().week
df.head(5)

# %%
#identification de la list des intérims
list_interim = ['TECHNICIEN MONTEUR1', 'ELECTROTECHNICIEN INDUSTRIEL2', 'MANUTENTIONNAIRE INDUSTRIEL1','ELECTROTECHNICIEN INDUSTRIEL1']

#inverser le prénom et le nom en conservant la liste interim dans le même sens 
def invert_name(name) :
    if name in list_interim :
        return name
    else : 
        invert = ' '.join(name.split()[1:] + [name.split()[0]])
        return invert

df["Prénom, NOM"] = df["Prénom, NOM"].apply(invert_name)
#Renomage de la colonne en NOM_PRENOM
df.rename(columns ={"Prénom, NOM":"NOM_Prénom"}, inplace = True)

# %%
#Il y a plusieur colonne heure travaillé, nous renomons la première en HEURE_DECLAREE
df.columns.values[5] = "HEURE_DECLAREE"

# %%
#Transfomation de Heure déclaré en float
df["HEURE_DECLAREE"] = df["HEURE_DECLAREE"].astype(float)

# %%
#Transformation de la colone panier en binaire 1 = oui pour pouvoir la compter
df["Panier"] = df["Panier"].fillna(0)
df["Panier"] = df["Panier"].apply(lambda x : 1 if x =="oui" else 0)


# %%
#Compter les jours travailler si la valeur est différente de 1 = jours travaillé
df["JOUR_TRAVAILLEE"] = df["HEURE_DECLAREE"].apply(lambda x : 0 if x == 0 else 1)

# %%
#le DF est aggrégé par Année, Mois , et nuémro de semaine avec une somme par semaine sur heure déclarée et les paniers
df_agg = df.groupby(["Annee","Mois","N_semaine","NOM_Prénom"])[["HEURE_DECLAREE","Panier","JOUR_TRAVAILLEE"]].sum().reset_index()
#Supprésion de l'année 2022
df_agg = df_agg[df_agg["Annee"] > 2022]
#Triage par année et numéro de semaine 
df_agg = df_agg.sort_values(["Annee","NOM_Prénom","N_semaine"])
df_agg

# %%
# Obtenir les jours fériés en France pour les années du DataFrame
years = df_agg["Annee"].unique()
french_holidays = holidays.France(years=years)

# Fonction pour calculer les jours ouvrés par semaine
def get_working_days_per_week(year, week):
    start_date = pd.Timestamp(f"{year}-01-01") + pd.Timedelta(weeks=week - 1)
    end_date = start_date + pd.Timedelta(days=6)
    working_days = 0

    for day_offset in range(7):  # Parcourt les 7 jours de la semaine
        current_date = (start_date + pd.Timedelta(days=day_offset)).date()
        # Compte uniquement si c'est un jour ouvré (lundi à vendredi) et non un jour férié
        if current_date.weekday() < 5 and current_date not in french_holidays:
            working_days += 1

    return working_days

# Appliquer la fonction pour chaque ligne
df_agg["Jours_ouvrés"] = df_agg.apply(lambda row: get_working_days_per_week(row["Annee"], row["N_semaine"]), axis=1)

# Calculer le seuil d'heures travaillées en fonction des jours ouvrés
df_agg["Seuil_heures"] = df_agg["Jours_ouvrés"] * 7  # 7 heures par jour ouvré

# Calculer les heures supplémentaires
df_agg["Heures_supplementaires"] = df_agg["HEURE_DECLAREE"] - df_agg["Seuil_heures"]
df_agg["Heures_supplementaires"] = df_agg["Heures_supplementaires"].apply(lambda x: max(x, 0))  # Pas d'heures supp négatives

df_agg


# %%
#Convertis les point en virgule pour google sheet
df_agg = df_agg.applymap( lambda x: f"{x:.2f}".replace('.', ',') if isinstance(x, float) else x)

# %%
#exportation des données dans google sheet
spreadsheet_export = gc.open('2024-2026_TABLEAU DES HEURES')
#Choisis lapremière feuille
worksheet = spreadsheet_export[0]

#selectionnne la cellule d'arrivé
worksheet.set_dataframe(df_agg, (1, 1))


